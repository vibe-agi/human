package tui

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	workmirror "github.com/vibe-agi/human/internal/mirror"
)

// MirrorWorkspace is the worker-local scratch view for one caller workspace.
// The caller tree remains authoritative; mutations are emitted as CAS-protected
// tool calls only after the operator reviews and confirms them.
type MirrorWorkspace interface {
	Dir() string
	ReconcileRequest(canonical.Request) (workmirror.ReconcileReport, error)
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
	sessionKey string
	namespace  string
	changes    []workmirror.Change
	err        error
}

type mirrorConfirmationReady struct {
	sessionKey string
	changes    []workmirror.Change
	calls      []completion.ToolCall
	err        error
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
		report, err := workspace.ReconcileRequest(assignment.Request)
		return mirrorPrepared{
			sessionKey: assignment.SessionKey(),
			namespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
			workspace:  workspace,
			report:     report,
			err:        err,
		}
	}
}

func reviewMirror(workspace MirrorWorkspace, assignment completion.Assignment) tea.Cmd {
	return func() tea.Msg {
		changes, err := workspace.Review()
		return mirrorReviewReady{
			sessionKey: assignment.SessionKey(),
			namespace:  mirrorNamespace(assignment.CallerID, assignment.WorkspaceKey),
			changes:    changes,
			err:        err,
		}
	}
}

func confirmMirror(
	workspace MirrorWorkspace,
	assignment completion.Assignment,
	previewed []workmirror.Change,
	calls []completion.ToolCall,
) tea.Cmd {
	return func() tea.Msg {
		current, err := workspace.Review()
		if err == nil && !sameChanges(previewed, current) {
			err = fmt.Errorf("mirror changed after preview; review it again before sending")
		}
		return mirrorConfirmationReady{
			sessionKey: assignment.SessionKey(), changes: current, calls: calls, err: err,
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
	return assignment.HarnessID == adapter.HumanShimID &&
		assignment.HarnessVersion == adapter.HumanShimVersion &&
		profile.HarnessID == adapter.HumanShimID &&
		profile.HarnessVersion == adapter.HumanShimVersion &&
		profile.Write != nil && profile.Write.Name == "human_write_file" &&
		profile.Edit != nil && profile.Edit.Name == "human_edit_file" &&
		profile.Delete != nil && profile.Delete.Name == "human_delete_file"
}

func validateMirrorCalls(request canonical.Request, calls []completion.ToolCall) error {
	declared := make(map[string]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		declared[tool.Name] = struct{}{}
	}
	for _, call := range calls {
		if _, ok := declared[call.Name]; !ok {
			return fmt.Errorf("caller did not declare required tool %q", call.Name)
		}
	}
	return nil
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
	if len(report.Confirmed) > 0 {
		return fmt.Sprintf("mirror reconciled · %d caller result(s) confirmed", len(report.Confirmed))
	}
	return "mirror ready"
}
