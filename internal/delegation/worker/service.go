// Package worker coordinates the human-side delegation authority with local
// git worktrees. It is deliberately outside the humand authority package: git
// artifacts cross that boundary only as opaque bytes and JSON metadata.
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/worktree"
)

const (
	metadataSchema = "human-agent.git-patch.v1"
)

var (
	ErrWorkerMismatch = errors.New("delegation task belongs to a different worker")
	ErrLocalDiverged  = errors.New("delegation authority and local worktree diverged")
	ErrBadMetadata    = errors.New("delegation artifact metadata is invalid")
)

// Authority is the opaque, CAS-guarded server-side boundary used by a human
// worker. delegation.Store satisfies it without learning anything about git.
type Authority interface {
	GetTask(context.Context, string) (delegation.Task, error)
	GetArtifact(context.Context, string, int64) (delegation.Artifact, error)
	AcceptTask(context.Context, delegation.AcceptTaskInput) (delegation.TransitionResult, error)
	DeliverTurn(context.Context, delegation.DeliverTurnInput) (delegation.DeliveryResult, error)
	CompleteTask(context.Context, delegation.CommandInput) (delegation.TransitionResult, error)
	ConfirmRewind(context.Context, delegation.CommandInput) (delegation.TransitionResult, error)
}

// Worktrees is the human-machine side-effect boundary. worktree.Engine
// satisfies it; tests can inject authority failures without shelling out.
type Worktrees interface {
	Create(context.Context, string, string) (worktree.Task, error)
	Load(string) (worktree.Task, error)
	DiscardCreated(context.Context, string) error
	CommitTurn(context.Context, string, int) (worktree.Artifact, error)
	CurrentArtifact(context.Context, string, int) (worktree.Artifact, error)
	ValidateComplete(context.Context, string) (worktree.Task, error)
	Complete(context.Context, string) (worktree.ArchiveReceipt, error)
	RewindWithReceipt(context.Context, string, int, string) (worktree.Task, worktree.RewindReceipt, error)
	RestoreRewind(context.Context, worktree.RewindReceipt) (worktree.Task, error)
}

type Config struct {
	Authority Authority
	Worktrees Worktrees
	WorkerID  string
}

type Service struct {
	authority Authority
	worktrees Worktrees
	workerID  string
}

func New(config Config) (*Service, error) {
	workerID := strings.TrimSpace(config.WorkerID)
	if config.Authority == nil || config.Worktrees == nil || workerID == "" {
		return nil, errors.New("delegation authority, worktrees, and worker id are required")
	}
	return &Service{authority: config.Authority, worktrees: config.Worktrees, workerID: workerID}, nil
}

type AcceptInput struct {
	TaskID           string
	ExpectedRevision int64
	BaseCommit       string
	Data             []byte
}

type AcceptResult struct {
	Task     delegation.Task
	Worktree worktree.Task
	Replay   bool
}

func (service *Service) Accept(ctx context.Context, input AcceptInput) (AcceptResult, error) {
	if strings.TrimSpace(input.TaskID) == "" || input.ExpectedRevision < 1 || strings.TrimSpace(input.BaseCommit) == "" {
		return AcceptResult{}, errors.New("task id, positive expected revision, and base commit are required")
	}
	authoritative, err := service.authority.GetTask(ctx, input.TaskID)
	if err != nil {
		return AcceptResult{}, err
	}
	if acceptedReplay(authoritative, input.ExpectedRevision, service.workerID) {
		local, err := service.worktrees.Load(input.TaskID)
		if err != nil {
			return AcceptResult{}, fmt.Errorf("load accepted task worktree: %w", err)
		}
		if local.BaseCommit != input.BaseCommit {
			return AcceptResult{}, fmt.Errorf("%w: accepted base %s differs from local base %s", ErrLocalDiverged, input.BaseCommit, local.BaseCommit)
		}
		return AcceptResult{Task: authoritative, Worktree: local, Replay: true}, nil
	}
	if authoritative.Revision != input.ExpectedRevision {
		return AcceptResult{}, revisionConflict(authoritative, input.ExpectedRevision)
	}
	if authoritative.State != delegation.StateSubmitted {
		return AcceptResult{}, fmt.Errorf("%w: cannot accept task in state %q", delegation.ErrInvalidTransition, authoritative.State)
	}

	local, err := service.ensureCreated(ctx, input.TaskID, input.BaseCommit)
	if err != nil {
		return AcceptResult{}, err
	}
	accepted, acceptErr := service.authority.AcceptTask(ctx, delegation.AcceptTaskInput{
		CommandInput: delegation.CommandInput{
			TaskID: input.TaskID, ExpectedRevision: input.ExpectedRevision, Data: bytes.Clone(input.Data),
		},
		WorkerID: service.workerID,
	})
	if acceptErr == nil {
		return AcceptResult{Task: accepted.Task, Worktree: local}, nil
	}
	// A response can be lost after the authority commits. Never compensate an
	// accept that is already authoritative.
	current, getErr := service.authority.GetTask(ctx, input.TaskID)
	if getErr != nil {
		return AcceptResult{}, errors.Join(
			acceptErr,
			fmt.Errorf("accept outcome is ambiguous; local worktree retained for reconciliation: %w", getErr),
		)
	}
	if acceptedReplay(current, input.ExpectedRevision, service.workerID) {
		return AcceptResult{Task: current, Worktree: local, Replay: true}, nil
	}
	if cleanupErr := service.worktrees.DiscardCreated(ctx, input.TaskID); cleanupErr != nil {
		return AcceptResult{}, errors.Join(acceptErr, fmt.Errorf("clean rejected task worktree: %w", cleanupErr))
	}
	return AcceptResult{}, acceptErr
}

type DeliverInput struct {
	TaskID           string
	ExpectedRevision int64
	Data             []byte
}

type DeliverResult struct {
	Task             delegation.Task
	StoredArtifact   delegation.Artifact
	WorktreeArtifact worktree.Artifact
	Replay           bool
}

// ArtifactMetadata makes cumulative-patch replace semantics self-contained.
// IncrementalPatch is retained for the caller's level-2 fallback.
type ArtifactMetadata struct {
	Schema           string          `json:"schema"`
	TaskID           string          `json:"task_id"`
	Turn             int             `json:"turn"`
	BaseCommit       string          `json:"base_commit"`
	Commit           string          `json:"commit"`
	PreviousCommit   string          `json:"previous_commit,omitempty"`
	IncrementalPatch []byte          `json:"incremental_patch,omitempty"`
	Files            []worktree.File `json:"files"`
}

func (service *Service) Deliver(ctx context.Context, input DeliverInput) (DeliverResult, error) {
	if strings.TrimSpace(input.TaskID) == "" || input.ExpectedRevision < 1 {
		return DeliverResult{}, errors.New("task id and positive expected revision are required")
	}
	authoritative, err := service.authority.GetTask(ctx, input.TaskID)
	if err != nil {
		return DeliverResult{}, err
	}
	if authoritative.State == delegation.StateInputRequired &&
		authoritative.Revision == input.ExpectedRevision+1 &&
		authoritative.LatestTurn == authoritative.NextTurn-1 {
		return service.replayDelivery(ctx, authoritative, input.ExpectedRevision)
	}
	if authoritative.Revision != input.ExpectedRevision {
		return DeliverResult{}, revisionConflict(authoritative, input.ExpectedRevision)
	}
	if authoritative.State != delegation.StateWorking {
		return DeliverResult{}, fmt.Errorf("%w: cannot deliver task in state %q", delegation.ErrInvalidTransition, authoritative.State)
	}
	if authoritative.WorkerID != service.workerID {
		return DeliverResult{}, ErrWorkerMismatch
	}
	localTask, err := service.worktrees.Load(input.TaskID)
	if err != nil {
		return DeliverResult{}, err
	}
	turn := int(authoritative.NextTurn)
	var localArtifact worktree.Artifact
	localReplay := false
	switch {
	case int64(localTask.LatestTurn) == authoritative.LatestTurn &&
		int64(localTask.NextTurn) == authoritative.NextTurn:
		localArtifact, err = service.worktrees.CommitTurn(ctx, input.TaskID, turn)
	case localTask.LatestTurn == turn && localTask.NextTurn == turn+1 &&
		int64(localTask.LatestTurn) == authoritative.LatestTurn+1:
		// The previous attempt committed locally, then failed before authority
		// acknowledgement. Rebuild byte-identical artifacts without committing.
		localArtifact, err = service.worktrees.CurrentArtifact(ctx, input.TaskID, turn)
		localReplay = true
	default:
		err = fmt.Errorf("%w: local latest turn %d, authority latest turn %d", ErrLocalDiverged, localTask.LatestTurn, authoritative.LatestTurn)
	}
	if err != nil {
		return DeliverResult{}, err
	}
	metadata, err := encodeMetadata(localArtifact)
	if err != nil {
		return DeliverResult{}, err
	}
	artifactID := artifactID(localArtifact)
	delivered, deliverErr := service.authority.DeliverTurn(ctx, delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{
			TaskID: input.TaskID, ExpectedRevision: input.ExpectedRevision, Data: bytes.Clone(input.Data),
		},
		ArtifactID: artifactID, ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData: bytes.Clone(localArtifact.CumulativePatch), ArtifactMetadata: metadata,
	})
	if deliverErr == nil {
		return DeliverResult{
			Task: delivered.Task, StoredArtifact: delivered.Artifact,
			WorktreeArtifact: localArtifact, Replay: localReplay,
		}, nil
	}
	if replay, ok := service.reconcileDelivery(ctx, input.ExpectedRevision, localArtifact, artifactID, metadata); ok {
		return replay, nil
	}
	return DeliverResult{}, deliverErr
}

type ConfirmRewindInput struct {
	TaskID           string
	ExpectedRevision int64
	TargetTurn       int64
	Data             []byte
}

type CompleteInput struct {
	TaskID           string
	ExpectedRevision int64
	Data             []byte
}

type CompleteResult struct {
	Task    delegation.Task
	Archive worktree.ArchiveReceipt
	Replay  bool
}

// Complete preflights the local delivery anchor before making the authority
// terminal, then archives the worktree. If cleanup fails after the authority
// commits, an exact retry enters the completed branch and finishes the
// idempotent local archive instead of running the transition again.
func (service *Service) Complete(ctx context.Context, input CompleteInput) (CompleteResult, error) {
	if strings.TrimSpace(input.TaskID) == "" || input.ExpectedRevision < 1 {
		return CompleteResult{}, errors.New("task id and positive expected revision are required")
	}
	authoritative, err := service.authority.GetTask(ctx, input.TaskID)
	if err != nil {
		return CompleteResult{}, err
	}
	if authoritative.WorkerID != service.workerID {
		return CompleteResult{}, ErrWorkerMismatch
	}
	if authoritative.State == delegation.StateCompleted {
		archive, err := service.worktrees.Complete(ctx, input.TaskID)
		if err != nil {
			return CompleteResult{}, fmt.Errorf("finish completed task archive: %w", err)
		}
		return CompleteResult{Task: authoritative, Archive: archive, Replay: true}, nil
	}
	if authoritative.Revision != input.ExpectedRevision {
		return CompleteResult{}, revisionConflict(authoritative, input.ExpectedRevision)
	}
	if authoritative.State != delegation.StateWorking && authoritative.State != delegation.StateInputRequired {
		return CompleteResult{}, fmt.Errorf("%w: cannot complete task in state %q", delegation.ErrInvalidTransition, authoritative.State)
	}
	local, err := service.worktrees.ValidateComplete(ctx, input.TaskID)
	if err != nil {
		return CompleteResult{}, fmt.Errorf("preflight completed task worktree: %w", err)
	}
	if int64(local.LatestTurn) != authoritative.LatestTurn || int64(local.NextTurn) != authoritative.NextTurn {
		return CompleteResult{}, fmt.Errorf("%w: local turn state does not match authority", ErrLocalDiverged)
	}
	transition, transitionErr := service.authority.CompleteTask(ctx, delegation.CommandInput{
		TaskID: input.TaskID, ExpectedRevision: input.ExpectedRevision, Data: bytes.Clone(input.Data),
	})
	replay := false
	if transitionErr != nil {
		current, getErr := service.authority.GetTask(ctx, input.TaskID)
		if getErr != nil || current.State != delegation.StateCompleted || current.WorkerID != service.workerID ||
			current.Revision != input.ExpectedRevision+1 {
			return CompleteResult{}, transitionErr
		}
		transition.Task = current
		replay = true
	}
	archive, err := service.worktrees.Complete(ctx, input.TaskID)
	if err != nil {
		return CompleteResult{}, fmt.Errorf("authority completed task at revision %d; archive retry required: %w",
			transition.Task.Revision, err)
	}
	return CompleteResult{Task: transition.Task, Archive: archive, Replay: replay}, nil
}

type ConfirmRewindResult struct {
	Task     delegation.Task
	Worktree worktree.Task
	Replay   bool
}

func (service *Service) ConfirmRewind(ctx context.Context, input ConfirmRewindInput) (ConfirmRewindResult, error) {
	if strings.TrimSpace(input.TaskID) == "" || input.ExpectedRevision < 1 || input.TargetTurn < 0 {
		return ConfirmRewindResult{}, errors.New("task id, positive expected revision, and non-negative target are required")
	}
	authoritative, err := service.authority.GetTask(ctx, input.TaskID)
	if err != nil {
		return ConfirmRewindResult{}, err
	}
	if rewindReplay(authoritative, input) {
		if authoritative.WorkerID != service.workerID {
			return ConfirmRewindResult{}, ErrWorkerMismatch
		}
		local, err := service.worktrees.Load(input.TaskID)
		if err != nil {
			return ConfirmRewindResult{}, err
		}
		targetCommit, err := service.rewindTargetCommit(ctx, local, input.TargetTurn)
		if err != nil {
			return ConfirmRewindResult{}, err
		}
		if int64(local.LatestTurn) != input.TargetTurn || local.LatestCommit != targetCommit {
			return ConfirmRewindResult{}, fmt.Errorf("%w: confirmed rewind is not reflected locally", ErrLocalDiverged)
		}
		return ConfirmRewindResult{Task: authoritative, Worktree: local, Replay: true}, nil
	}
	if authoritative.Revision != input.ExpectedRevision {
		return ConfirmRewindResult{}, revisionConflict(authoritative, input.ExpectedRevision)
	}
	if authoritative.State != delegation.StateRewindPending || authoritative.PendingRewindTo == nil ||
		*authoritative.PendingRewindTo != input.TargetTurn {
		return ConfirmRewindResult{}, fmt.Errorf("%w: task has no matching pending rewind", delegation.ErrInvalidTransition)
	}
	if authoritative.WorkerID != service.workerID {
		return ConfirmRewindResult{}, ErrWorkerMismatch
	}
	local, err := service.worktrees.Load(input.TaskID)
	if err != nil {
		return ConfirmRewindResult{}, err
	}
	if int64(local.LatestTurn) != authoritative.LatestTurn || int64(local.NextTurn) != authoritative.NextTurn {
		return ConfirmRewindResult{}, fmt.Errorf("%w: local turn state does not match authority", ErrLocalDiverged)
	}
	targetCommit, err := service.rewindTargetCommit(ctx, local, input.TargetTurn)
	if err != nil {
		return ConfirmRewindResult{}, err
	}
	rewound, receipt, err := service.worktrees.RewindWithReceipt(
		ctx, input.TaskID, int(input.TargetTurn), targetCommit,
	)
	if err != nil {
		return ConfirmRewindResult{}, err
	}
	confirmed, confirmErr := service.authority.ConfirmRewind(ctx, delegation.CommandInput{
		TaskID: input.TaskID, ExpectedRevision: input.ExpectedRevision, Data: bytes.Clone(input.Data),
	})
	if confirmErr == nil {
		return ConfirmRewindResult{Task: confirmed.Task, Worktree: rewound}, nil
	}
	// As with delivery, distinguish an acknowledged CAS failure from a lost
	// response after commit before compensating local git state.
	current, getErr := service.authority.GetTask(ctx, input.TaskID)
	if getErr == nil && rewindReplay(current, input) {
		return ConfirmRewindResult{Task: current, Worktree: rewound, Replay: true}, nil
	}
	if getErr != nil && !errors.Is(confirmErr, delegation.ErrRevisionConflict) {
		return ConfirmRewindResult{}, errors.Join(
			confirmErr,
			fmt.Errorf("rewind outcome is ambiguous; local rewind retained for reconciliation: %w", getErr),
		)
	}
	restored, restoreErr := service.worktrees.RestoreRewind(ctx, receipt)
	if restoreErr != nil {
		return ConfirmRewindResult{}, errors.Join(confirmErr, fmt.Errorf("restore rewind backup: %w", restoreErr))
	}
	_ = restored
	return ConfirmRewindResult{}, confirmErr
}

func (service *Service) ensureCreated(ctx context.Context, taskID, baseCommit string) (worktree.Task, error) {
	local, err := service.worktrees.Load(taskID)
	if err == nil {
		if local.BaseCommit != baseCommit || local.LatestTurn != 0 || local.NextTurn != 1 {
			return worktree.Task{}, fmt.Errorf("%w: orphaned accept worktree does not match request", ErrLocalDiverged)
		}
		return local, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return worktree.Task{}, err
	}
	return service.worktrees.Create(ctx, taskID, baseCommit)
}

func (service *Service) replayDelivery(
	ctx context.Context,
	task delegation.Task,
	expectedRevision int64,
) (DeliverResult, error) {
	if task.WorkerID != service.workerID || task.LatestTurn < 1 {
		return DeliverResult{}, ErrWorkerMismatch
	}
	localArtifact, err := service.worktrees.CurrentArtifact(ctx, task.ID, int(task.LatestTurn))
	if err != nil {
		return DeliverResult{}, err
	}
	metadata, err := encodeMetadata(localArtifact)
	if err != nil {
		return DeliverResult{}, err
	}
	result, ok := service.reconcileDelivery(
		ctx, expectedRevision, localArtifact, artifactID(localArtifact), metadata,
	)
	if !ok {
		return DeliverResult{}, fmt.Errorf("%w: authoritative delivery differs from local artifact", ErrLocalDiverged)
	}
	return result, nil
}

func (service *Service) reconcileDelivery(
	ctx context.Context,
	expectedRevision int64,
	local worktree.Artifact,
	wantedID string,
	metadata []byte,
) (DeliverResult, bool) {
	task, err := service.authority.GetTask(ctx, local.TaskID)
	if err != nil || task.State != delegation.StateInputRequired ||
		task.Revision != expectedRevision+1 || task.LatestTurn != int64(local.Turn) ||
		task.LatestTurn != task.NextTurn-1 ||
		task.WorkerID != service.workerID {
		return DeliverResult{}, false
	}
	stored, err := service.authority.GetArtifact(ctx, local.TaskID, int64(local.Turn))
	if err != nil || stored.ID != wantedID || stored.MediaType != delegation.GitPatchMediaType ||
		!bytes.Equal(stored.Data, local.CumulativePatch) || !bytes.Equal(stored.Metadata, metadata) {
		return DeliverResult{}, false
	}
	return DeliverResult{
		Task: task, StoredArtifact: stored, WorktreeArtifact: local, Replay: true,
	}, true
}

func (service *Service) rewindTargetCommit(
	ctx context.Context,
	local worktree.Task,
	target int64,
) (string, error) {
	if target == 0 {
		return local.BaseCommit, nil
	}
	artifact, err := service.authority.GetArtifact(ctx, local.ID, target)
	if err != nil {
		return "", err
	}
	if artifact.MediaType != delegation.GitPatchMediaType {
		return "", fmt.Errorf("%w: rewind target has media type %q", ErrBadMetadata, artifact.MediaType)
	}
	if artifact.Superseded() {
		return "", fmt.Errorf("%w: rewind target artifact is superseded", delegation.ErrInvalidRewind)
	}
	metadata, err := DecodeArtifactMetadata(artifact.Metadata)
	if err != nil {
		return "", err
	}
	if metadata.TaskID != local.ID || int64(metadata.Turn) != target ||
		metadata.BaseCommit != local.BaseCommit || strings.TrimSpace(metadata.Commit) == "" {
		return "", fmt.Errorf("%w: target metadata does not match task/turn/base", ErrBadMetadata)
	}
	return metadata.Commit, nil
}

func encodeMetadata(artifact worktree.Artifact) ([]byte, error) {
	metadata := ArtifactMetadata{
		Schema: metadataSchema, TaskID: artifact.TaskID, Turn: artifact.Turn,
		BaseCommit: artifact.BaseCommit, Commit: artifact.Commit,
		PreviousCommit:   artifact.PreviousCommit,
		IncrementalPatch: bytes.Clone(artifact.IncrementalPatch),
		Files:            append([]worktree.File(nil), artifact.Files...),
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode delegation artifact metadata: %w", err)
	}
	return payload, nil
}

func DecodeArtifactMetadata(payload []byte) (ArtifactMetadata, error) {
	var metadata ArtifactMetadata
	if err := json.Unmarshal(payload, &metadata); err != nil {
		return ArtifactMetadata{}, fmt.Errorf("%w: %v", ErrBadMetadata, err)
	}
	if metadata.Schema != metadataSchema || strings.TrimSpace(metadata.TaskID) == "" ||
		metadata.Turn < 1 || !validObjectID(metadata.BaseCommit) ||
		!validObjectID(metadata.Commit) || !validObjectID(metadata.PreviousCommit) {
		return ArtifactMetadata{}, ErrBadMetadata
	}
	seen := make(map[string]struct{}, len(metadata.Files))
	for _, file := range metadata.Files {
		if !validMetadataFile(file) {
			return ArtifactMetadata{}, ErrBadMetadata
		}
		if _, duplicate := seen[file.Path]; duplicate {
			return ArtifactMetadata{}, ErrBadMetadata
		}
		seen[file.Path] = struct{}{}
	}
	return metadata, nil
}

func validMetadataFile(file worktree.File) bool {
	if file.Path == "" || file.Path != strings.TrimSpace(file.Path) || path.Clean(file.Path) != file.Path ||
		strings.Contains(file.Path, "\\") || strings.HasPrefix(file.Path, "/") ||
		strings.EqualFold(file.Path, ".git") || strings.HasPrefix(strings.ToLower(file.Path), ".git/") {
		return false
	}
	switch file.Mode {
	case "000000":
		return file.BlobSHA == "deleted"
	case "100644", "100755":
		return validObjectID(file.BlobSHA)
	default:
		return false
	}
}

func validObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, digit := range value {
		if digit >= '0' && digit <= '9' {
			continue
		}
		if digit < 'a' || digit > 'f' {
			return false
		}
	}
	return true
}

func artifactID(artifact worktree.Artifact) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(artifact.TaskID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(fmt.Sprintf("%d\x00%s", artifact.Turn, artifact.Commit)))
	return "artifact_" + hex.EncodeToString(hash.Sum(nil))
}

func acceptedReplay(task delegation.Task, expectedRevision int64, workerID string) bool {
	return task.State == delegation.StateWorking && task.WorkerID == workerID &&
		task.Revision == expectedRevision+1 && task.LatestTurn == 0 && task.NextTurn == 1
}

func rewindReplay(task delegation.Task, input ConfirmRewindInput) bool {
	return task.State == delegation.StateInputRequired && task.PendingRewindTo == nil &&
		task.LatestTurn == input.TargetTurn && task.LatestTurn < task.NextTurn-1 &&
		task.Revision == input.ExpectedRevision+1
}

func revisionConflict(task delegation.Task, expected int64) error {
	return fmt.Errorf(
		"%w: task %q is at revision %d, expected %d",
		delegation.ErrRevisionConflict, task.ID, task.Revision, expected,
	)
}
