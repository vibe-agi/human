package mirror

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/safety"
)

const discardedToolResultDigest = "discarded-before-outbox"

type capabilityKind string

const (
	capabilityRead   capabilityKind = "read"
	capabilitySearch capabilityKind = "search"
	capabilityWrite  capabilityKind = "write"
	capabilityEdit   capabilityKind = "edit"
	capabilityDelete capabilityKind = "delete"
	capabilityRename capabilityKind = "rename"
	capabilityExec   capabilityKind = "exec"
)

type profileCapability struct {
	kind capabilityKind
	tool *adapter.Tool
}

// BuildReport carries calls separately from explicit correctness downgrades.
// Callers must present Warnings to the operator before delivery.
type BuildReport struct {
	// Changes is positionally aligned with Calls. It can be a strict subset of
	// the reviewed batch when an exact adapter lacks a non-destructive mapping
	// for one operation (currently delete). Callers must record delivery intents
	// from this slice, never from the original batch.
	Changes  []Change
	Calls    []completion.ToolCall
	Warnings []string
}

// RecordDeliveryIntents durably binds reviewed mutations to their exact
// tool-call IDs before the event enters the worker outbox. A later caller
// result can therefore advance the caller baseline to the content that was
// actually delivered even when the Human has already saved a newer draft.
func (workspace *Workspace) RecordDeliveryIntents(
	changes []Change,
	calls []completion.ToolCall,
	profile *adapter.Profile,
	workspaceRoot string,
) error {
	if err := validateWorkspaceProfile(profile, true); err != nil {
		return err
	}
	if len(changes) == 0 || len(changes) != len(calls) {
		return errors.New("delivery intent requires one tool call for every reviewed change")
	}
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	staged := make(map[string]deliveryIntent, len(workspace.state.Deliveries)+len(changes))
	for ledgerID, intent := range workspace.state.Deliveries {
		staged[ledgerID] = intent
	}
	for index, change := range changes {
		call := calls[index]
		if strings.TrimSpace(call.ID) == "" {
			return errors.New("delivery intent tool call has no stable id")
		}
		toolPath, err := profileToolPath(*profile, workspaceRoot, change.Path)
		if err != nil {
			return err
		}
		expected, _, err := buildProfileCall(*profile, change, toolPath)
		if err != nil {
			return err
		}
		expected.ID = call.ID
		expectedDigest, err := deliveryToolCallDigest(expected)
		if err != nil {
			return err
		}
		actualDigest, err := deliveryToolCallDigest(call)
		if err != nil {
			return err
		}
		if expectedDigest != actualDigest {
			return fmt.Errorf("delivery tool call %s no longer matches reviewed change %s", call.ID, change.Path)
		}
		relative, target, err := workspace.resolve(change.Path)
		if err != nil {
			return err
		}
		tracked, hasBaseline := workspace.state.Entries[relative]
		switch change.Kind {
		case ChangeWrite:
			if hasBaseline || change.ExpectedSHA != callerfs.AbsentFingerprint {
				return fmt.Errorf("delivery baseline changed before write %s", relative)
			}
		case ChangeEdit, ChangeDelete:
			if !hasBaseline || tracked.Fingerprint != change.ExpectedSHA {
				return fmt.Errorf("delivery baseline changed before %s %s", change.Kind, relative)
			}
		default:
			return fmt.Errorf("unsupported delivery intent kind %q", change.Kind)
		}
		if change.Kind == ChangeDelete {
			if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
				if err == nil {
					return fmt.Errorf("mirror changed after delete review: %s exists", relative)
				}
				return err
			}
		} else {
			current, err := os.ReadFile(target)
			if err != nil {
				return err
			}
			if !bytes.Equal(current, change.NewContent) {
				return fmt.Errorf("mirror changed after review for %s", relative)
			}
		}
		intent := deliveryIntent{
			ProfileKey: profile.Key(), ToolName: call.Name, Path: relative,
			Kind: change.Kind, CallDigest: actualDigest,
			BaseFingerprint: change.ExpectedSHA, Deleted: change.Kind == ChangeDelete,
		}
		if !intent.Deleted {
			fingerprint := callerfs.Fingerprint(change.NewContent)
			intent.Delivered, err = workspace.storeBlob(change.NewContent, fingerprint)
			if err != nil {
				return err
			}
		}
		ledgerID := resultLedgerID(*profile, call.ID)
		if previous, exists := staged[ledgerID]; exists && previous != intent {
			return fmt.Errorf("delivery tool call %s was already recorded with different intent", call.ID)
		}
		if _, processed := workspace.state.Results[ledgerID]; processed {
			return fmt.Errorf("delivery tool call %s was already reconciled", call.ID)
		}
		staged[ledgerID] = intent
	}
	original := workspace.state.Deliveries
	workspace.state.Deliveries = staged
	if err := workspace.save(); err != nil {
		workspace.state.Deliveries = original
		return err
	}
	return nil
}

func deliveryToolCallDigest(call completion.ToolCall) (string, error) {
	payload, err := json.Marshal(struct {
		ID        string         `json:"id"`
		Namespace string         `json:"namespace"`
		Name      string         `json:"name"`
		Input     map[string]any `json:"input"`
	}{ID: call.ID, Namespace: call.Namespace, Name: call.Name, Input: call.Input})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func deliveryToolUseDigest(use canonical.Block) (string, error) {
	return deliveryToolCallDigest(completion.ToolCall{
		ID: use.ToolCallID, Namespace: use.ToolNamespace, Name: use.ToolName, Input: use.Input,
	})
}

func (workspace *Workspace) confirmRecordedDelivery(
	profile adapter.Profile,
	use canonical.Block,
	remoteFingerprint string,
	deleted bool,
) (bool, error) {
	ledgerID := resultLedgerID(profile, use.ToolCallID)
	useDigest, err := deliveryToolUseDigest(use)
	if err != nil {
		return false, err
	}
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	intent, exists := workspace.state.Deliveries[ledgerID]
	if !exists {
		return false, nil
	}
	if intent.ProfileKey != profile.Key() || intent.ToolName != use.ToolName ||
		intent.CallDigest != useDigest || intent.Deleted != deleted {
		return false, errors.New("caller tool result does not match its recorded delivery intent")
	}
	current, hasCurrent := workspace.state.Entries[intent.Path]
	alreadyApplied := !deleted && hasCurrent && current.Fingerprint == intent.Delivered.Fingerprint ||
		deleted && !hasCurrent
	if !alreadyApplied {
		if intent.BaseFingerprint == callerfs.AbsentFingerprint {
			if hasCurrent {
				return false, errors.New("caller baseline advanced before the recorded delivery result")
			}
		} else if !hasCurrent || current.Fingerprint != intent.BaseFingerprint {
			return false, errors.New("caller baseline changed before the recorded delivery result")
		}
	}
	if !deleted {
		if intent.Delivered.Fingerprint == "" || intent.Delivered.Blob == "" {
			return false, errors.New("recorded delivery has no delivered baseline")
		}
		if remoteFingerprint != "" && remoteFingerprint != intent.Delivered.Fingerprint {
			return false, errors.New("caller result fingerprint does not match the recorded delivery")
		}
		workspace.state.Entries[intent.Path] = intent.Delivered
	} else {
		delete(workspace.state.Entries, intent.Path)
	}
	delete(workspace.state.Warnings, intent.Path)
	if err := workspace.save(); err != nil {
		return false, err
	}
	return true, nil
}

// BuildHydrationToolCallForProfile builds the versioned bootstrap path used
// when a native OpenCode read result is only a lossy display. OpenCode's own
// debug file reader emits exact base64 bytes without calling a model. The
// command still runs through the client Agent's normal bash permission gate.
func BuildHydrationToolCallForProfile(
	relativePath string,
	profile *adapter.Profile,
	workspaceRoot string,
) (completion.ToolCall, error) {
	if err := validateWorkspaceProfile(profile, false); err != nil {
		return completion.ToolCall{}, err
	}
	if profile.Key() != adapter.OpenCodeID+"@"+adapter.OpenCodeVersion ||
		profile.ResultCodec != adapter.ResultOpenCode11718 || profile.Exec == nil {
		return completion.ToolCall{}, fmt.Errorf(
			"exact workspace pull is not supported by adapter %s", profile.Key(),
		)
	}
	target, err := profileToolPath(*profile, workspaceRoot, relativePath)
	if err != nil {
		return completion.ToolCall{}, err
	}
	root, err := cleanAbsoluteRoot(workspaceRoot)
	if err != nil {
		return completion.ToolCall{}, err
	}
	commandPath, err := filepath.Rel(root, target)
	if err != nil || commandPath == "." || commandPath == ".." ||
		strings.HasPrefix(commandPath, ".."+string(filepath.Separator)) {
		return completion.ToolCall{}, ErrMirrorEscape
	}
	commandPath = filepath.ToSlash(commandPath)
	if strings.HasPrefix(commandPath, "-") {
		// OpenCode 1.17.18 does not honor `--` for this positional, so keep a
		// legitimate leading-dash file from being parsed as a CLI option.
		commandPath = "./" + commandPath
	}
	commandField, err := requiredField(*profile, &profile.Exec.Tool, capabilityExec, "command")
	if err != nil {
		return completion.ToolCall{}, err
	}
	callID, err := canonical.NewOpaqueID("tool_")
	if err != nil {
		return completion.ToolCall{}, err
	}
	input := map[string]any{
		commandField: "opencode debug file read --pure " + shellSingleQuote(commandPath),
	}
	if profile.Exec.CWDField != "" {
		input[profile.Exec.CWDField] = root
	}
	return completion.ToolCall{ID: callID, Name: profile.Exec.Name, Input: input}, nil
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func (workspace *Workspace) RecordHydrationIntent(
	relativePath string,
	call completion.ToolCall,
	profile *adapter.Profile,
	workspaceRoot string,
) error {
	expected, err := BuildHydrationToolCallForProfile(relativePath, profile, workspaceRoot)
	if err != nil {
		return err
	}
	expected.ID = call.ID
	expectedDigest, err := deliveryToolCallDigest(expected)
	if err != nil {
		return err
	}
	actualDigest, err := deliveryToolCallDigest(call)
	if err != nil {
		return err
	}
	if expectedDigest != actualDigest {
		return errors.New("workspace pull call does not match its versioned adapter command")
	}
	relative, _, err := workspace.resolve(relativePath)
	if err != nil {
		return err
	}
	intent := hydrationIntent{
		ProfileKey: profile.Key(), ToolName: call.Name, Path: relative, CallDigest: actualDigest,
	}
	ledgerID := resultLedgerID(*profile, call.ID)
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	if previous, exists := workspace.state.Hydrations[ledgerID]; exists && previous != intent {
		return errors.New("workspace pull tool-call id was reused with different intent")
	}
	if _, processed := workspace.state.Results[ledgerID]; processed {
		return errors.New("workspace pull tool call was already reconciled")
	}
	original, existed := workspace.state.Hydrations[ledgerID]
	workspace.state.Hydrations[ledgerID] = intent
	if err := workspace.save(); err != nil {
		if existed {
			workspace.state.Hydrations[ledgerID] = original
		} else {
			delete(workspace.state.Hydrations, ledgerID)
		}
		return err
	}
	return nil
}

// DiscardToolIntents removes local provenance only after an event is known not
// to have entered (or to have been permanently removed from) the durable
// worker outbox. It also writes a terminal ledger tombstone for every exact
// call, including calls whose intent was never created. That tombstone closes
// the cross-store crash window where mirror cleanup commits before the worker
// pending-send row is deleted: a restart can never record and send that old
// call again. Digests prevent a stale UI callback from deleting a newer intent
// that reused the same tool-call ID with different input.
func (workspace *Workspace) DiscardToolIntents(
	calls []completion.ToolCall,
	profile *adapter.Profile,
) error {
	if profile == nil || strings.TrimSpace(profile.HarnessID) == "" ||
		strings.TrimSpace(profile.HarnessVersion) == "" {
		return errors.New("discard tool intents requires an exact adapter profile")
	}
	// Reconciliation holds this lock across its processed check, baseline
	// mutation and result mark. Taking it first gives discard a single linear
	// order with a late caller result; a sentinel can never be overwritten after
	// cleanup has reported success.
	workspace.reconcileMu.Lock()
	defer workspace.reconcileMu.Unlock()
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	deliveries := make(map[string]deliveryIntent, len(workspace.state.Deliveries))
	for key, intent := range workspace.state.Deliveries {
		deliveries[key] = intent
	}
	hydrations := make(map[string]hydrationIntent, len(workspace.state.Hydrations))
	for key, intent := range workspace.state.Hydrations {
		hydrations[key] = intent
	}
	results := make(map[string]string, len(workspace.state.Results)+len(calls))
	for key, digest := range workspace.state.Results {
		results[key] = digest
	}
	changed := false
	for _, call := range calls {
		digest, err := deliveryToolCallDigest(call)
		if err != nil {
			return err
		}
		ledgerID := resultLedgerID(*profile, call.ID)
		_, processed := results[ledgerID]
		if intent, exists := deliveries[ledgerID]; exists {
			if intent.ProfileKey != profile.Key() || intent.ToolName != call.Name || intent.CallDigest != digest {
				return errors.New("delivery intent changed before discard")
			}
			delete(deliveries, ledgerID)
			changed = true
		}
		if intent, exists := hydrations[ledgerID]; exists {
			if intent.ProfileKey != profile.Key() || intent.ToolName != call.Name || intent.CallDigest != digest {
				return errors.New("hydration intent changed before discard")
			}
			delete(hydrations, ledgerID)
			changed = true
		}
		if processed {
			// A real result that linearized before cleanup is already terminal and
			// markResult normally removed both intents. Keep that digest rather than
			// pretending the observed caller result was never delivered. A prior
			// discard sentinel is simply idempotent.
			continue
		}
		results[ledgerID] = discardedToolResultDigest
		changed = true
	}
	if !changed {
		return nil
	}
	originalDeliveries, originalHydrations, originalResults :=
		workspace.state.Deliveries, workspace.state.Hydrations, workspace.state.Results
	workspace.state.Deliveries, workspace.state.Hydrations, workspace.state.Results =
		deliveries, hydrations, results
	if err := workspace.save(); err != nil {
		workspace.state.Deliveries, workspace.state.Hydrations, workspace.state.Results =
			originalDeliveries, originalHydrations, originalResults
		return err
	}
	return nil
}

func (workspace *Workspace) reconcileRecordedHydration(
	profile adapter.Profile,
	use canonical.Block,
	output string,
) (bool, error) {
	ledgerID := resultLedgerID(profile, use.ToolCallID)
	useDigest, err := deliveryToolUseDigest(use)
	if err != nil {
		return false, err
	}
	workspace.mu.Lock()
	intent, exists := workspace.state.Hydrations[ledgerID]
	workspace.mu.Unlock()
	if !exists {
		return false, nil
	}
	if intent.ProfileKey != profile.Key() || intent.ToolName != use.ToolName || intent.CallDigest != useDigest {
		return true, errors.New("workspace pull result does not match its recorded intent")
	}
	var payload struct {
		Content  *string `json:"content"`
		Encoding string  `json:"encoding"`
		Mime     string  `json:"mime"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		return true, fmt.Errorf("decode exact OpenCode workspace pull: %w", err)
	}
	if payload.Encoding != "base64" || payload.Content == nil {
		return true, errors.New("exact OpenCode workspace pull did not return base64 content")
	}
	content, err := base64.StdEncoding.DecodeString(*payload.Content)
	if err != nil {
		return true, fmt.Errorf("decode exact OpenCode workspace bytes: %w", err)
	}
	if err := workspace.Hydrate(intent.Path, content, callerfs.Fingerprint(content)); err != nil {
		return true, err
	}
	return true, nil
}

// BuildToolCallsForProfile converts reviewed changes using only fields and
// semantics declared by profile. It never inspects caller tool schemas.
func BuildToolCallsForProfile(
	changes []Change,
	profile *adapter.Profile,
	workspaceRoot string,
) (BuildReport, error) {
	report := BuildReport{}
	if err := validateWorkspaceProfile(profile, true); err != nil {
		return report, err
	}
	report.Calls = make([]completion.ToolCall, 0, len(changes))
	report.Changes = make([]Change, 0, len(changes))
	for _, change := range changes {
		if change.Warning == safety.SeverityBlock {
			return BuildReport{}, fmt.Errorf(
				"blocked mirror change %s: %s", change.Path, strings.Join(change.Reasons, "; "),
			)
		}
		path, err := profileToolPath(*profile, workspaceRoot, change.Path)
		if err != nil {
			return BuildReport{}, err
		}
		call, warnings, err := buildProfileCall(*profile, change, path)
		if err != nil {
			if change.Kind == ChangeDelete && profile.Delete == nil {
				report.Warnings = appendUnique(report.Warnings, fmt.Sprintf(
					"adapter %s cannot deliver deletion %s: no explicitly mapped delete tool; deletion remains pending",
					profile.Key(), change.Path,
				))
				continue
			}
			return BuildReport{}, err
		}
		id, err := canonical.NewOpaqueID("tool_")
		if err != nil {
			return BuildReport{}, err
		}
		call.ID = id
		report.Changes = append(report.Changes, change)
		report.Calls = append(report.Calls, call)
		for _, warning := range warnings {
			report.Warnings = appendUnique(report.Warnings, warning)
		}
	}
	return report, nil
}

func buildProfileCall(
	profile adapter.Profile,
	change Change,
	path string,
) (completion.ToolCall, []string, error) {
	switch change.Kind {
	case ChangeWrite:
		return buildProfileWrite(profile, change, path, "")
	case ChangeEdit:
		if profile.Edit != nil && profile.Edit.Semantics == adapter.EditExact &&
			utf8.Valid(change.OldContent) && utf8.Valid(change.NewContent) {
			return buildProfileEdit(profile, change, path)
		}
		reason := "exact edit capability is unavailable"
		if profile.Edit != nil && profile.Edit.Semantics != adapter.EditExact {
			reason = fmt.Sprintf("edit semantics are %q, not exact", profile.Edit.Semantics)
		} else if !utf8.Valid(change.OldContent) || !utf8.Valid(change.NewContent) {
			reason = "edit content is not valid UTF-8"
		}
		return buildProfileWrite(profile, change, path,
			fmt.Sprintf("adapter %s downgraded edit %s to full-file write: %s", profile.Key(), change.Path, reason))
	case ChangeDelete:
		return buildProfileDelete(profile, change, path)
	default:
		return completion.ToolCall{}, nil, fmt.Errorf("unknown mirror change kind %q", change.Kind)
	}
}

func buildProfileWrite(
	profile adapter.Profile,
	change Change,
	path string,
	downgradeWarning string,
) (completion.ToolCall, []string, error) {
	if profile.Write == nil {
		return completion.ToolCall{}, nil, fmt.Errorf(
			"workspace delivery disabled for %s: change %s requires an explicitly mapped write tool",
			profile.Key(), change.Path,
		)
	}
	pathField, err := requiredField(profile, profile.Write, capabilityWrite, "path")
	if err != nil {
		return completion.ToolCall{}, nil, err
	}
	contentField, err := requiredField(profile, profile.Write, capabilityWrite, "content")
	if err != nil {
		return completion.ToolCall{}, nil, err
	}
	input := map[string]any{pathField: path}
	if utf8.Valid(change.NewContent) {
		input[contentField] = string(change.NewContent)
		if field := profile.Write.Args["encoding"]; field != "" {
			input[field] = "utf-8"
		}
	} else {
		encodingField := profile.Write.Args["encoding"]
		if encodingField == "" {
			return completion.ToolCall{}, nil, fmt.Errorf(
				"workspace delivery disabled for %s: write tool %q has no binary encoding field for %s",
				profile.Key(), profile.Write.Name, change.Path,
			)
		}
		input[contentField] = base64.StdEncoding.EncodeToString(change.NewContent)
		input[encodingField] = "base64"
	}
	warnings := []string{}
	if downgradeWarning != "" {
		warnings = append(warnings, downgradeWarning)
	}
	if field := profile.Write.Args["expected_sha256"]; field != "" {
		expected := change.ExpectedSHA
		if expected == "" && change.Kind == ChangeWrite {
			expected = callerfs.AbsentFingerprint
		}
		input[field] = expected
	} else {
		warnings = append(warnings, mutationWithoutCASWarning(profile, capabilityWrite, change.Path))
	}
	return completion.ToolCall{Name: profile.Write.Name, Input: input}, warnings, nil
}

func buildProfileEdit(
	profile adapter.Profile,
	change Change,
	path string,
) (completion.ToolCall, []string, error) {
	edit := profile.Edit
	pathField, err := requiredField(profile, &edit.Tool, capabilityEdit, "path")
	if err != nil {
		return completion.ToolCall{}, nil, err
	}
	if edit.OldField == "" || edit.NewField == "" ||
		edit.Args["old"] != edit.OldField || edit.Args["new"] != edit.NewField {
		return completion.ToolCall{}, nil, fmt.Errorf(
			"workspace delivery disabled for %s: edit tool %q has inconsistent old/new field mapping",
			profile.Key(), edit.Name,
		)
	}
	input := map[string]any{
		pathField:     path,
		edit.OldField: string(change.OldContent),
		edit.NewField: string(change.NewContent),
	}
	warnings := []string{}
	if field := edit.Args["expected_sha256"]; field != "" {
		input[field] = change.ExpectedSHA
	} else {
		warnings = append(warnings, mutationWithoutCASWarning(profile, capabilityEdit, change.Path))
	}
	return completion.ToolCall{Name: edit.Name, Input: input}, warnings, nil
}

func buildProfileDelete(
	profile adapter.Profile,
	change Change,
	path string,
) (completion.ToolCall, []string, error) {
	if profile.Delete == nil {
		return completion.ToolCall{}, nil, fmt.Errorf(
			"workspace delivery disabled for %s: delete %s requires an explicitly mapped delete tool",
			profile.Key(), change.Path,
		)
	}
	pathField, err := requiredField(profile, profile.Delete, capabilityDelete, "path")
	if err != nil {
		return completion.ToolCall{}, nil, err
	}
	input := map[string]any{pathField: path}
	warnings := []string{}
	if field := profile.Delete.Args["expected_sha256"]; field != "" {
		input[field] = change.ExpectedSHA
	} else {
		warnings = append(warnings, mutationWithoutCASWarning(profile, capabilityDelete, change.Path))
	}
	return completion.ToolCall{Name: profile.Delete.Name, Input: input}, warnings, nil
}

func requiredField(
	profile adapter.Profile,
	tool *adapter.Tool,
	kind capabilityKind,
	semantic string,
) (string, error) {
	field := strings.TrimSpace(tool.Args[semantic])
	if field == "" {
		return "", fmt.Errorf(
			"workspace delivery disabled for %s: %s tool %q has no explicit %s field mapping",
			profile.Key(), kind, tool.Name, semantic,
		)
	}
	return field, nil
}

func mutationWithoutCASWarning(profile adapter.Profile, kind capabilityKind, path string) string {
	return fmt.Sprintf(
		"adapter %s %s for %s has no expected_sha256 mapping; caller mutation is not CAS-protected",
		profile.Key(), kind, path,
	)
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// ReconcileRequestForProfile consumes previously unseen caller results using
// only the exact tool names, field mappings, path style, and result codec in
// profile. Unrelated tool results are ignored; schemas are never inspected.
func (workspace *Workspace) ReconcileRequestForProfile(
	request canonical.Request,
	profile *adapter.Profile,
	workspaceRoot string,
) (ReconcileReport, error) {
	workspace.reconcileMu.Lock()
	defer workspace.reconcileMu.Unlock()
	report := ReconcileReport{Failed: make(map[string]string)}
	if err := validateWorkspaceProfile(profile, false); err != nil {
		report.Warnings = append(report.Warnings, err.Error())
		return report, nil
	}
	uses := make(map[string]canonical.Block)
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type == canonical.BlockToolUse {
				if _, duplicate := uses[block.ToolCallID]; duplicate {
					return report, fmt.Errorf("duplicate tool use id %s in reconciliation history", block.ToolCallID)
				}
				uses[block.ToolCallID] = block
			}
		}
	}
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type != canonical.BlockToolResult {
				continue
			}
			use, known := uses[block.ToolCallID]
			if !known {
				continue
			}
			// Exact adapter profile capabilities are ordinary top-level functions.
			// A namespaced function with the same leaf name is a distinct tool and
			// must not confirm a recorded delivery or enter the local-content
			// fallback reconciliation path.
			if use.ToolNamespace != "" {
				continue
			}
			capability, mapped := mappedCapability(*profile, use.ToolName)
			if !mapped {
				continue
			}
			digest, err := profileToolResultDigest(*profile, use, block)
			if err != nil {
				return report, err
			}
			ledgerID := resultLedgerID(*profile, block.ToolCallID)
			workspace.mu.Lock()
			previous, processed := workspace.state.Results[ledgerID]
			workspace.mu.Unlock()
			if processed {
				if previous == discardedToolResultDigest {
					report.Failed[block.ToolCallID] =
						"tool result arrived after its delivery was discarded before outbox; baseline was not advanced"
					continue
				}
				if previous != digest {
					return report, fmt.Errorf("tool result %s changed after reconciliation", block.ToolCallID)
				}
				continue
			}
			outcome := workspace.reconcileProfileResult(
				*profile, workspaceRoot, capability, use, block,
			)
			if outcome.failure != "" {
				report.Failed[block.ToolCallID] = outcome.failure
			}
			if outcome.warning != "" {
				report.Warnings = appendUnique(report.Warnings,
					block.ToolCallID+": "+outcome.warning)
			}
			if outcome.err != nil {
				report.Failed[block.ToolCallID] = outcome.err.Error()
				continue
			}
			if outcome.mark {
				if err := workspace.markResult(ledgerID, digest); err != nil {
					return report, err
				}
			}
			if outcome.confirmed {
				report.Confirmed = append(report.Confirmed, block.ToolCallID)
			}
		}
	}
	return report, nil
}

type reconcileOutcome struct {
	confirmed bool
	mark      bool
	warning   string
	failure   string
	err       error
}

func (workspace *Workspace) reconcileProfileResult(
	profile adapter.Profile,
	workspaceRoot string,
	capability profileCapability,
	use canonical.Block,
	result canonical.Block,
) reconcileOutcome {
	switch profile.ResultCodec {
	case adapter.ResultHumanShimV1:
		return workspace.reconcileHumanShimResult(profile, workspaceRoot, capability, use, result)
	case adapter.ResultOpenCode11718:
		return workspace.reconcileOpenCode11718Result(profile, workspaceRoot, capability, use, result)
	default:
		return reconcileOutcome{err: fmt.Errorf(
			"workspace reconciliation disabled for %s: unsupported result codec %q",
			profile.Key(), profile.ResultCodec,
		)}
	}
}

func (workspace *Workspace) reconcileHumanShimResult(
	profile adapter.Profile,
	workspaceRoot string,
	capability profileCapability,
	use canonical.Block,
	result canonical.Block,
) reconcileOutcome {
	content, isError, errorMessage, err := decodeToolResponse(result.Output, result.IsError)
	if err != nil {
		return reconcileOutcome{err: err}
	}
	if isError {
		if errorMessage == "" {
			errorMessage = "caller tool returned an error"
		}
		return reconcileOutcome{failure: errorMessage, mark: true}
	}
	switch capability.kind {
	case capabilityWrite, capabilityEdit:
		confirmed, err := workspace.confirmRecordedDelivery(
			profile, use, stringValue(content["sha256"]), false,
		)
		if err != nil {
			return reconcileOutcome{err: err}
		}
		if confirmed {
			return reconcileOutcome{confirmed: true, mark: true}
		}
	case capabilityDelete:
		confirmed, err := workspace.confirmRecordedDelivery(profile, use, "", true)
		if err != nil {
			return reconcileOutcome{err: err}
		}
		if confirmed {
			return reconcileOutcome{confirmed: true, mark: true}
		}
	}
	pathFrom := func(semantic, resultField string) (string, error) {
		raw := stringValue(content[resultField])
		if raw == "" {
			field, fieldErr := requiredField(profile, capability.tool, capability.kind, semantic)
			if fieldErr != nil {
				return "", fieldErr
			}
			raw = stringValue(use.Input[field])
		}
		return profileMirrorPath(profile, workspaceRoot, raw)
	}
	var applyErr error
	switch capability.kind {
	case capabilityRead:
		path, err := pathFrom("path", "path")
		if err != nil {
			applyErr = err
			break
		}
		data, err := decodeMirrorContent(stringValue(content["content"]), stringValue(content["encoding"]))
		if err != nil {
			applyErr = err
			break
		}
		applyErr = workspace.Hydrate(path, data, stringValue(content["sha256"]))
	case capabilityWrite, capabilityEdit:
		path, err := pathFrom("path", "path")
		if err != nil {
			applyErr = err
			break
		}
		applyErr = workspace.Confirm(path, stringValue(content["sha256"]), false)
	case capabilityDelete:
		path, err := pathFrom("path", "path")
		if err != nil {
			applyErr = err
			break
		}
		applyErr = workspace.Confirm(path, "", true)
	case capabilityRename:
		from, err := pathFrom("from", "from")
		if err != nil {
			applyErr = err
			break
		}
		to, err := pathFrom("to", "to")
		if err != nil {
			applyErr = err
			break
		}
		applyErr = workspace.ConfirmRename(from, to, stringValue(content["sha256"]))
	case capabilitySearch, capabilityExec:
	default:
		applyErr = fmt.Errorf("unsupported profile capability %q", capability.kind)
	}
	if applyErr != nil {
		return reconcileOutcome{err: applyErr}
	}
	return reconcileOutcome{confirmed: true, mark: true}
}

func (workspace *Workspace) reconcileOpenCode11718Result(
	profile adapter.Profile,
	workspaceRoot string,
	capability profileCapability,
	use canonical.Block,
	result canonical.Block,
) reconcileOutcome {
	text, ok := result.Output.(string)
	if !ok {
		return reconcileOutcome{err: errors.New("OpenCode tool result is not text")}
	}
	if result.IsError {
		return reconcileOutcome{failure: strings.TrimSpace(text), mark: true}
	}
	switch capability.kind {
	case capabilityRead:
		// The captured OpenCode display result adds line numbers and cannot
		// distinguish a trailing newline; it also carries no content hash.
		// Hydrating it would invent a byte-exact caller baseline.
		return reconcileOutcome{
			mark: true,
			warning: "read result was not hydrated because OpenCode 1.17.18 returns " +
				"lossy display text without a remote sha256",
		}
	case capabilityWrite, capabilityEdit:
		want := "Wrote file successfully."
		if capability.kind == capabilityEdit {
			want = "Edit applied successfully."
		}
		if strings.TrimSpace(text) != want {
			return reconcileOutcome{
				failure: fmt.Sprintf("OpenCode %s result did not match captured success text", capability.kind),
				mark:    true,
			}
		}
		confirmed, err := workspace.confirmRecordedDelivery(profile, use, "", false)
		if err != nil {
			return reconcileOutcome{err: err}
		}
		if confirmed {
			return reconcileOutcome{
				confirmed: true,
				mark:      true,
				warning: "baseline advanced from the exact recorded delivery intent; OpenCode 1.17.18 " +
					"returns no remote sha256, so this is not CAS proof",
			}
		}
		pathField, err := requiredField(profile, capability.tool, capability.kind, "path")
		if err != nil {
			return reconcileOutcome{err: err}
		}
		path, err := profileMirrorPath(profile, workspaceRoot, stringValue(use.Input[pathField]))
		if err != nil {
			return reconcileOutcome{err: err}
		}
		content, err := workspace.verifyOpenCodeMutationIntent(profile, capability, use, path)
		if err != nil {
			return reconcileOutcome{err: err}
		}
		if err := workspace.Confirm(path, callerfs.Fingerprint(content), false); err != nil {
			return reconcileOutcome{err: err}
		}
		return reconcileOutcome{
			confirmed: true,
			mark:      true,
			warning: "baseline advanced from reviewed local content; OpenCode 1.17.18 " +
				"returns no remote sha256, so this is not CAS proof",
		}
	case capabilitySearch, capabilityExec:
		if capability.kind == capabilityExec {
			hydrated, err := workspace.reconcileRecordedHydration(profile, use, text)
			if err != nil {
				if hydrated {
					return reconcileOutcome{failure: err.Error(), mark: true}
				}
				return reconcileOutcome{err: err}
			}
			if hydrated {
				return reconcileOutcome{
					confirmed: true, mark: true,
					warning: "workspace file hydrated from exact base64 bytes returned by the versioned OpenCode helper",
				}
			}
		}
		return reconcileOutcome{confirmed: true, mark: true}
	default:
		return reconcileOutcome{err: fmt.Errorf(
			"OpenCode 1.17.18 has no captured %s result contract", capability.kind,
		)}
	}
}

func (workspace *Workspace) verifyOpenCodeMutationIntent(
	profile adapter.Profile,
	capability profileCapability,
	use canonical.Block,
	path string,
) ([]byte, error) {
	changes, err := workspace.Review()
	if err != nil {
		return nil, err
	}
	var pending *Change
	for index := range changes {
		if changes[index].Path == filepath.ToSlash(path) {
			pending = &changes[index]
			break
		}
	}
	if pending == nil {
		return nil, fmt.Errorf(
			"OpenCode %s result has no matching reviewed mirror change for %s",
			capability.kind, path,
		)
	}
	if pending.Warning == safety.SeverityBlock {
		return nil, fmt.Errorf("OpenCode result cannot confirm blocked mirror change %s", path)
	}
	switch capability.kind {
	case capabilityWrite:
		contentField, err := requiredField(profile, capability.tool, capabilityWrite, "content")
		if err != nil {
			return nil, err
		}
		content, ok := use.Input[contentField].(string)
		if !ok || !utf8.Valid(pending.NewContent) || content != string(pending.NewContent) {
			return nil, fmt.Errorf(
				"OpenCode write result content does not match the reviewed mirror change for %s", path,
			)
		}
	case capabilityEdit:
		if pending.Kind != ChangeEdit || profile.Edit == nil {
			return nil, fmt.Errorf("OpenCode edit result does not match an exact pending edit for %s", path)
		}
		oldValue, oldOK := use.Input[profile.Edit.OldField].(string)
		newValue, newOK := use.Input[profile.Edit.NewField].(string)
		if !oldOK || !newOK || !utf8.Valid(pending.OldContent) || !utf8.Valid(pending.NewContent) ||
			oldValue != string(pending.OldContent) || newValue != string(pending.NewContent) {
			return nil, fmt.Errorf(
				"OpenCode edit result old/new content does not match the reviewed mirror change for %s", path,
			)
		}
	default:
		return nil, fmt.Errorf("OpenCode %s is not a workspace mutation", capability.kind)
	}
	return pending.NewContent, nil
}

func mappedCapability(profile adapter.Profile, name string) (profileCapability, bool) {
	tools := []profileCapability{
		{capabilityRead, profile.Read},
		{capabilitySearch, profile.Search},
		{capabilityWrite, profile.Write},
		{capabilityDelete, profile.Delete},
		{capabilityRename, profile.Rename},
	}
	if profile.Edit != nil {
		tools = append(tools, profileCapability{capabilityEdit, &profile.Edit.Tool})
	}
	if profile.Exec != nil {
		tools = append(tools, profileCapability{capabilityExec, &profile.Exec.Tool})
	}
	for _, capability := range tools {
		if capability.tool != nil && capability.tool.Name == name {
			return capability, true
		}
	}
	return profileCapability{}, false
}

func validateWorkspaceProfile(profile *adapter.Profile, delivery bool) error {
	action := "reconciliation"
	if delivery {
		action = "delivery"
	}
	if profile == nil {
		return fmt.Errorf("workspace %s disabled: no exact adapter profile", action)
	}
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("workspace %s disabled for %s: invalid adapter profile: %w", action, profile.Key(), err)
	}
	if profile.PathStyle == "" || profile.ResultCodec == "" {
		return fmt.Errorf(
			"workspace %s disabled for %s: profile has no explicit path/result contract",
			action, profile.Key(),
		)
	}
	return nil
}

func profileToolPath(profile adapter.Profile, workspaceRoot, relative string) (string, error) {
	relative = filepath.ToSlash(strings.TrimSpace(relative))
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("%w: %s", ErrMirrorEscape, relative)
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s", ErrMirrorEscape, relative)
	}
	relative = filepath.ToSlash(clean)
	switch profile.PathStyle {
	case adapter.PathWorkspaceVirtual:
		return "/workspace/" + relative, nil
	case adapter.PathAbsolute:
		root, err := cleanAbsoluteRoot(workspaceRoot)
		if err != nil {
			return "", fmt.Errorf("workspace delivery disabled for %s: %w", profile.Key(), err)
		}
		target := filepath.Join(root, filepath.FromSlash(relative))
		inside, err := filepath.Rel(root, target)
		if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
			return "", ErrMirrorEscape
		}
		return target, nil
	default:
		return "", fmt.Errorf("workspace delivery disabled for %s: unsupported path style %q", profile.Key(), profile.PathStyle)
	}
}

func profileMirrorPath(profile adapter.Profile, workspaceRoot, path string) (string, error) {
	path = strings.TrimSpace(path)
	switch profile.PathStyle {
	case adapter.PathWorkspaceVirtual:
		return path, nil
	case adapter.PathAbsolute:
		root, err := cleanAbsoluteRoot(workspaceRoot)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(path) {
			return "", fmt.Errorf("%w: adapter %s returned non-absolute path %q", ErrMirrorEscape, profile.Key(), path)
		}
		relative, err := filepath.Rel(root, filepath.Clean(path))
		if err != nil || relative == "." || relative == ".." ||
			strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("%w: adapter path %q is outside workspace root", ErrMirrorEscape, path)
		}
		return filepath.ToSlash(relative), nil
	default:
		return "", fmt.Errorf("unsupported path style %q", profile.PathStyle)
	}
}

func cleanAbsoluteRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" || !filepath.IsAbs(root) {
		return "", errors.New("an absolute caller workspace root is required")
	}
	return filepath.Clean(root), nil
}

func profileToolResultDigest(
	profile adapter.Profile,
	use canonical.Block,
	result canonical.Block,
) (string, error) {
	value := any(result)
	if profile.HarnessID != adapter.HumanShimID || profile.HarnessVersion != adapter.HumanShimVersion {
		// Native harness results such as OpenCode's fixed success text contain
		// no mutation payload. Bind the durable digest to the exact tool use so
		// a rewritten path/content with the same call ID cannot replay as valid.
		value = struct {
			Use    canonical.Block `json:"use"`
			Result canonical.Block `json:"result"`
		}{Use: use, Result: result}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func resultLedgerID(profile adapter.Profile, toolCallID string) string {
	if profile.HarnessID == adapter.HumanShimID && profile.HarnessVersion == adapter.HumanShimVersion {
		return toolCallID // preserve the on-disk human-shim@1 ledger namespace
	}
	return "adapter:" + profile.Key() + ":" + toolCallID
}
