package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/safety"
	workmirror "github.com/vibe-agi/human/internal/mirror"
)

// MirrorWorkspace is the worker-local scratch view for one caller workspace.
// The caller tree remains authoritative; mutations are emitted as CAS-protected
// tool calls only after the operator reviews and confirms them.
type MirrorWorkspace interface {
	Dir() string
	ReconcileRequestForProfile(
		canonical.Request, *adapter.Profile, string,
	) (workmirror.ReconcileReport, error)
	Review() ([]workmirror.Change, error)
}

// MirrorManager opens a mirror by its correctness namespace. Implementations
// must not include task or UI conversation identifiers in that namespace.
type MirrorManager interface {
	Open(callerID, workspaceKey string) (MirrorWorkspace, error)
}

type filesystemMirrorManager struct {
	root string
	mu   sync.Mutex
	open map[string]MirrorWorkspace
}

func newFilesystemMirrorManager(root string) *filesystemMirrorManager {
	return &filesystemMirrorManager{root: root, open: make(map[string]MirrorWorkspace)}
}

func (manager *filesystemMirrorManager) Open(callerID, workspaceKey string) (MirrorWorkspace, error) {
	namespace := mirrorNamespace(callerID, workspaceKey)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if workspace := manager.open[namespace]; workspace != nil {
		return workspace, nil
	}
	workspace, err := workmirror.Open(manager.root, callerID, workspaceKey)
	if err != nil {
		return nil, err
	}
	manager.open[namespace] = workspace
	return workspace, nil
}

type mirrorPrepared struct {
	sessionKey string
	namespace  string
	workspace  MirrorWorkspace
	report     workmirror.ReconcileReport
	err        error
}

type mirrorReviewReady struct {
	sessionKey  string
	namespace   string
	generation  uint64
	changes     []workmirror.Change
	diagnostics []workmirror.ReviewDiagnostic
	automatic   bool
	err         error
}

type mirrorConfirmationReady struct {
	sessionKey string
	namespace  string
	generation uint64
	eventID    string
	changes    []workmirror.Change
	calls      []completion.ToolCall
	err        error
}

type mirrorWatchState struct {
	events       <-chan workmirror.WatchEvent
	cancel       context.CancelFunc
	startID      uint64
	failures     int
	starting     bool
	retryPending bool
}

type mirrorWatchStarted struct {
	namespace string
	startID   uint64
	events    <-chan workmirror.WatchEvent
	cancel    context.CancelFunc
	err       error
}

type mirrorWatchRetryReady struct {
	namespace string
	startID   uint64
}

type mirrorWatchEvent struct {
	namespace string
	events    <-chan workmirror.WatchEvent
	event     workmirror.WatchEvent
	open      bool
}

func prepareMirror(manager MirrorManager, assignment completion.Assignment) tea.Cmd {
	return func() tea.Msg {
		workspace, err := manager.Open(assignment.CallerID, assignment.WorkspaceKey)
		if err != nil {
			return mirrorPrepared{
				sessionKey: assignment.SessionKey(),
				namespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
				err:        err,
			}
		}
		report, err := workspace.ReconcileRequestForProfile(
			assignment.Request, assignment.Adapter, assignment.Root,
		)
		return mirrorPrepared{
			sessionKey: assignment.SessionKey(),
			namespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
			workspace:  workspace,
			report:     report,
			err:        err,
		}
	}
}

func reviewMirror(
	workspace MirrorWorkspace,
	assignment completion.Assignment,
	generation uint64,
) tea.Cmd {
	return reviewMirrorSource(workspace, assignment, false, generation)
}

func reviewMirrorAutomatically(
	workspace MirrorWorkspace,
	assignment completion.Assignment,
	generation uint64,
) tea.Cmd {
	return reviewMirrorSource(workspace, assignment, true, generation)
}

func reviewMirrorSource(
	workspace MirrorWorkspace,
	assignment completion.Assignment,
	automatic bool,
	generation uint64,
) tea.Cmd {
	return func() tea.Msg {
		changes, err := workspace.Review()
		var diagnostics []workmirror.ReviewDiagnostic
		if reporter, ok := workspace.(interface {
			ReviewDiagnostics() []workmirror.ReviewDiagnostic
		}); ok {
			diagnostics = reporter.ReviewDiagnostics()
		}
		return mirrorReviewReady{
			sessionKey:  assignment.SessionKey(),
			namespace:   mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
			generation:  generation,
			changes:     changes,
			diagnostics: diagnostics,
			automatic:   automatic,
			err:         err,
		}
	}
}

const (
	mirrorWatchRetryMin = 100 * time.Millisecond
	mirrorWatchRetryMax = 5 * time.Second
)

func startMirrorWatch(workspace MirrorWorkspace, namespace string, startID uint64) tea.Cmd {
	return func() tea.Msg {
		watchable, ok := workspace.(interface {
			Watch(context.Context, time.Duration) (<-chan workmirror.WatchEvent, error)
		})
		if !ok {
			return mirrorWatchStarted{namespace: namespace, startID: startID}
		}
		ctx, cancel := context.WithCancel(context.Background())
		events, err := watchable.Watch(ctx, 150*time.Millisecond)
		if err != nil {
			cancel()
			return mirrorWatchStarted{namespace: namespace, startID: startID, err: err}
		}
		if events == nil {
			cancel()
			return mirrorWatchStarted{
				namespace: namespace,
				startID:   startID,
				err:       fmt.Errorf("workspace watcher returned a nil event stream"),
			}
		}
		return mirrorWatchStarted{
			namespace: namespace, startID: startID, events: events, cancel: cancel,
		}
	}
}

func waitToRestartMirrorWatch(namespace string, startID uint64, delay time.Duration) tea.Cmd {
	return func() tea.Msg {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		<-timer.C
		return mirrorWatchRetryReady{namespace: namespace, startID: startID}
	}
}

func mirrorWatchRetryDelay(failures int) time.Duration {
	if failures <= 1 {
		return mirrorWatchRetryMin
	}
	delay := mirrorWatchRetryMin
	for attempt := 1; attempt < failures; attempt++ {
		if delay >= mirrorWatchRetryMax/2 {
			return mirrorWatchRetryMax
		}
		delay *= 2
	}
	if delay > mirrorWatchRetryMax {
		return mirrorWatchRetryMax
	}
	return delay
}

func waitForMirrorChange(namespace string, events <-chan workmirror.WatchEvent) tea.Cmd {
	return func() tea.Msg {
		event, open := <-events
		return mirrorWatchEvent{namespace: namespace, events: events, event: event, open: open}
	}
}

func confirmMirror(
	workspace MirrorWorkspace,
	assignment completion.Assignment,
	previewed []workmirror.Change,
	calls []completion.ToolCall,
	generation uint64,
	eventID string,
) tea.Cmd {
	return func() tea.Msg {
		allCurrent, err := workspace.Review()
		current := selectReviewedChanges(allCurrent, previewed)
		if err == nil && !sameChanges(previewed, current) {
			err = fmt.Errorf("mirror changed after preview; review it again before sending")
		}
		if err == nil {
			if recorder, ok := workspace.(interface {
				RecordDeliveryIntents(
					[]workmirror.Change, []completion.ToolCall, *adapter.Profile, string,
				) error
			}); ok {
				err = recorder.RecordDeliveryIntents(
					current, calls, assignment.Adapter, assignment.Root,
				)
			}
		}
		return mirrorConfirmationReady{
			sessionKey: assignment.SessionKey(),
			namespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
			generation: generation,
			eventID:    eventID,
			changes:    current,
			calls:      calls,
			err:        err,
		}
	}
}

func selectReviewedChanges(current, previewed []workmirror.Change) []workmirror.Change {
	if len(previewed) == 0 {
		return nil
	}
	byPath := make(map[string]workmirror.Change, len(current))
	for _, change := range current {
		byPath[change.Path] = change
	}
	selected := make([]workmirror.Change, 0, len(previewed))
	for _, expected := range previewed {
		change, exists := byPath[expected.Path]
		if !exists {
			return selected
		}
		selected = append(selected, change)
	}
	return selected
}

func discardMirrorIntents(
	workspace MirrorWorkspace,
	calls []completion.ToolCall,
	profile *adapter.Profile,
	reason string,
) tea.Cmd {
	return func() tea.Msg {
		discarder, ok := workspace.(interface {
			DiscardToolIntents([]completion.ToolCall, *adapter.Profile) error
		})
		if !ok || len(calls) == 0 {
			return mirrorIntentsDiscarded{reason: reason}
		}
		return mirrorIntentsDiscarded{
			reason: reason,
			err:    discarder.DiscardToolIntents(calls, profile),
		}
	}
}

func mirrorEnabled(assignment completion.Assignment) bool {
	if assignment.CapabilityTier != completion.TierRemoteTools &&
		assignment.CapabilityTier != completion.TierWorkspace {
		return false
	}
	if assignment.CallerID == "" || assignment.WorkspaceKey == "" || assignment.Adapter == nil {
		return false
	}
	profile := assignment.Adapter
	if assignment.HarnessID != profile.HarnessID ||
		assignment.HarnessVersion != profile.HarnessVersion || profile.Validate() != nil ||
		profile.PathStyle == "" || profile.ResultCodec == "" {
		return false
	}
	if profile.PathStyle == adapter.PathAbsolute && strings.TrimSpace(assignment.Root) == "" {
		return false
	}
	return profile.Write != nil || profile.Edit != nil || profile.Delete != nil
}

func validateMirrorCalls(request canonical.Request, calls []completion.ToolCall) error {
	if request.ToolCallPolicy == canonical.ToolCallsSerial && len(calls) > 1 {
		return fmt.Errorf("caller allows one tool call per response; split this delivery")
	}
	type toolIdentity struct{ namespace, name string }
	declared := make(map[toolIdentity]canonical.Tool, len(request.Tools))
	for _, tool := range request.Tools {
		declared[toolIdentity{namespace: tool.Namespace, name: tool.Name}] = tool
	}
	for _, call := range calls {
		tool, ok := declared[toolIdentity{namespace: call.Namespace, name: call.Name}]
		if !ok {
			return fmt.Errorf("caller did not declare required tool %q", call.QualifiedName())
		}
		if err := validateMirrorToolInput(tool, call.Input); err != nil {
			return fmt.Errorf("caller tool %q no longer matches adapter contract: %w", call.QualifiedName(), err)
		}
	}
	return nil
}

func validateMirrorToolInput(tool canonical.Tool, input map[string]any) error {
	var schema struct {
		Type       string `json:"type"`
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
		return fmt.Errorf("invalid JSON schema: %w", err)
	}
	if schema.Type != "" && schema.Type != "object" {
		return fmt.Errorf("root schema must be an object")
	}
	// OpenAI-compatible callers may omit properties for a permissive object
	// tool. The exact adapter still supplies semantics; there is simply no
	// tighter caller schema to cross-check at this boundary.
	if schema.Properties == nil {
		return nil
	}
	for _, field := range schema.Required {
		if _, ok := input[field]; !ok {
			return fmt.Errorf("required field %q is missing", field)
		}
	}
	for field, value := range input {
		property, ok := schema.Properties[field]
		if !ok {
			return fmt.Errorf("field %q is not declared", field)
		}
		if !mirrorJSONTypeMatches(property.Type, value) {
			return fmt.Errorf("field %q does not match JSON type %q", field, property.Type)
		}
	}
	return nil
}

func mirrorJSONTypeMatches(kind string, value any) bool {
	switch kind {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "integer":
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		default:
			return false
		}
	case "number":
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			return true
		default:
			return false
		}
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}

func mirrorNamespace(callerID, workspaceKey string) string {
	return callerID + "\x00" + workspaceKey
}

func sameChanges(left, right []workmirror.Change) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Kind != right[index].Kind ||
			left[index].Path != right[index].Path ||
			left[index].ExpectedSHA != right[index].ExpectedSHA ||
			left[index].Warning != right[index].Warning ||
			!equalStrings(left[index].Reasons, right[index].Reasons) ||
			string(left[index].OldContent) != string(right[index].OldContent) ||
			string(left[index].NewContent) != string(right[index].NewContent) {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func reconcileSummary(report workmirror.ReconcileReport) string {
	if len(report.Failed) > 0 {
		ids := make([]string, 0, len(report.Failed))
		for id := range report.Failed {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		failures := make([]string, 0, len(ids))
		for _, id := range ids {
			failures = append(failures, id+": "+report.Failed[id])
		}
		return "caller result failed; baseline unchanged · " + strings.Join(failures, "; ")
	}
	if len(report.Warnings) > 0 {
		prefix := "mirror warning"
		if len(report.Confirmed) > 0 {
			prefix = fmt.Sprintf("mirror reconciled · %d confirmed · warning", len(report.Confirmed))
		}
		return prefix + ": " + strings.Join(report.Warnings, "; ")
	}
	if len(report.Confirmed) > 0 {
		return fmt.Sprintf("mirror reconciled · %d caller result(s) confirmed", len(report.Confirmed))
	}
	return "mirror ready"
}

func formatMirrorDiagnostics(diagnostics []workmirror.ReviewDiagnostic) string {
	if len(diagnostics) == 0 {
		return ""
	}
	first := diagnostics[0]
	message := fmt.Sprintf("skipped %s: %s", first.Path, first.Reason)
	if len(diagnostics) > 1 {
		message += fmt.Sprintf(" (+%d more)", len(diagnostics)-1)
	}
	return message
}

func changesNeedHumanReview(changes []workmirror.Change) bool {
	for _, change := range changes {
		if change.Warning != safety.SeverityAllow {
			return true
		}
	}
	return false
}
