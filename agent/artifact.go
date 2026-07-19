package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/workspace"
)

const maxRevisionBytes = 512

// FreezeArtifact records one immutable declarative workspace payload. The
// first Artifact observed for a Workspace initializes its confirmed head from
// the trusted caller adapter's ExpectedBaseRevision; later freezes must match
// that durable head exactly.
func (agent *Agent) FreezeArtifact(ctx context.Context, command FreezeArtifactCommand) (FreezeArtifactResult, error) {
	if err := validateCallContext(ctx); err != nil {
		return FreezeArtifactResult{}, err
	}
	if agent == nil {
		return FreezeArtifactResult{}, ErrClosed
	}
	command.Payload.Data = append([]byte(nil), command.Payload.Data...)
	if err := validateWorkerMeta(command.Meta, command.Task); err != nil {
		return FreezeArtifactResult{}, err
	}
	if err := validateTaskRef(command.Task); err != nil {
		return FreezeArtifactResult{}, err
	}
	if err := validateStable("artifact id", string(command.Artifact)); err != nil {
		return FreezeArtifactResult{}, err
	}
	if err := validateRevision("expected base revision", command.ExpectedBaseRevision); err != nil {
		return FreezeArtifactResult{}, err
	}
	if err := validateArtifactPayloadShape(command.Payload); err != nil {
		return FreezeArtifactResult{}, err
	}
	digest, err := commandDigest("freeze_artifact", command)
	if err != nil {
		return FreezeArtifactResult{}, err
	}
	store, release, err := agent.acquireStore()
	if err != nil {
		return FreezeArtifactResult{}, err
	}
	defer release()
	ref := ArtifactRef{Workspace: command.Task.Workspace, ID: command.Artifact}
	payloadDigest := digestPayload(command.Payload)
	resultRevision := nextRevision(command.ExpectedBaseRevision, ref, payloadDigest)
	artifactIdentity := Artifact{
		Ref: ref, Task: command.Task, State: ArtifactFrozen,
		BaseRevision: command.ExpectedBaseRevision, ResultRevision: resultRevision,
		Digest: digestArtifact(
			ref, command.Task, command.ExpectedBaseRevision, resultRevision,
			payloadDigest, command.Payload.MediaType,
		),
		PayloadDigest: payloadDigest, PayloadSize: int64(len(command.Payload.Data)),
		MediaType: command.Payload.MediaType,
	}

	var frozen FreezeArtifactResult
	var replayed bool
	var replayRecord StoreArtifactRecord
	err = store.View(ctx, func(view StoreView) error {
		var replay FreezeArtifactResult
		found, err := replayJSONCommandFromStore(
			view, command.Task.Workspace.Authority, command.Meta.ID,
			"freeze_artifact", digest, "freeze_artifact", &replay,
		)
		if err != nil || !found {
			return err
		}
		if err := validateFreezeResult(replay); err != nil {
			return err
		}
		if replay.Task.Ref != command.Task || replay.Artifact.Ref != ref {
			return fmt.Errorf("%w: frozen command result identity mismatch", ErrCorruptStore)
		}
		if _, err := verifyArtifactLeaseGrantHistory(view, command.Meta.Grant); err != nil {
			return err
		}
		stored, err := loadArtifactRecord(view, replay.Artifact.Ref)
		if err != nil || !sameArtifactIdentity(stored.Artifact, replay.Artifact) {
			return fmt.Errorf("%w: frozen command result does not match Artifact", ErrCorruptStore)
		}
		replayRecord = stored
		frozen, replayed = cloneFreezeResult(replay), true
		return nil
	})
	if err != nil {
		return FreezeArtifactResult{}, fmt.Errorf("preflight freeze Agent Artifact: %w", err)
	}
	if replayed {
		if _, err := agent.validateStoredValueShape(replayRecord.EncodedPayload, maxStoredArtifactBytes); err != nil {
			return FreezeArtifactResult{}, err
		}
		return cloneFreezeResult(frozen), nil
	}
	if int64(len(command.Payload.Data)) > agent.maxArtifactBytes {
		return FreezeArtifactResult{}, fmt.Errorf(
			"%w: Artifact payload must be 1..%d bytes", ErrInvalidArgument, agent.maxArtifactBytes,
		)
	}
	preparedPayload, err := agent.prepareArtifactPayload(ctx, artifactIdentity, command.Payload)
	if err != nil {
		return FreezeArtifactResult{}, err
	}
	var updateReplayed bool
	err = store.Update(ctx, func(tx StoreTx) error {
		var replay FreezeArtifactResult
		if found, err := replayJSONCommandFromStore(
			tx, command.Task.Workspace.Authority, command.Meta.ID,
			"freeze_artifact", digest, "freeze_artifact", &replay,
		); err != nil {
			return err
		} else if found {
			if err := validateFreezeResult(replay); err != nil {
				return err
			}
			wantRef := ArtifactRef{Workspace: command.Task.Workspace, ID: command.Artifact}
			if replay.Task.Ref != command.Task || replay.Artifact.Ref != wantRef {
				return fmt.Errorf("%w: frozen command result identity mismatch", ErrCorruptStore)
			}
			if _, err := verifyArtifactLeaseGrantHistory(tx, command.Meta.Grant); err != nil {
				return err
			}
			stored, err := loadArtifactRecord(tx, replay.Artifact.Ref)
			if err != nil || !sameArtifactIdentity(stored.Artifact, replay.Artifact) {
				return fmt.Errorf("%w: frozen command result does not match Artifact", ErrCorruptStore)
			}
			replayRecord = stored
			frozen = cloneFreezeResult(replay)
			updateReplayed = true
			return nil
		}
		currentRecord, err := loadTaskRecordFromStore(tx, command.Task)
		if err != nil {
			return err
		}
		if err := requireArtifactCurrentLease(tx, currentRecord, command.Meta.Grant); err != nil {
			return err
		}
		current := currentRecord.Task
		if current.Revision != command.Meta.ExpectedRevision {
			return &RevisionConflictError{
				Expected: command.Meta.ExpectedRevision, Actual: current.Revision,
			}
		}
		if current.State.Terminal() {
			return &TransitionError{Operation: "freeze_artifact", State: current.State, Terminal: true}
		}
		if current.State != TaskWorking {
			return &TransitionError{Operation: "freeze_artifact", State: current.State}
		}
		if current.Artifact != nil || current.Submission != nil {
			return ErrArtifactConflict
		}
		if current.Revision >= math.MaxInt64 || current.EventCount >= math.MaxInt64 {
			return fmt.Errorf("%w: task counters exhausted store integer range", ErrRevisionConflict)
		}

		now, err := checkedClockTime(agent.now)
		if err != nil {
			return err
		}
		now = timestampAtLeast(now, current.UpdatedAt)
		workspaceRef := command.Task.Workspace
		if _, err := tx.InsertWorkspaceHead(StoreWorkspaceHeadRecord{Head: WorkspaceHead{
			Workspace: workspaceRef, ConfirmedRevision: command.ExpectedBaseRevision, UpdatedAt: now,
		}}); err != nil {
			return fmt.Errorf("initialize Agent Workspace head: %w", err)
		}
		head, err := loadArtifactWorkspaceHead(tx, workspaceRef)
		if err != nil {
			return err
		}
		if head.ConfirmedRevision != command.ExpectedBaseRevision {
			return fmt.Errorf(
				"%w: expected %q, confirmed %q", ErrWorkspaceConflict,
				command.ExpectedBaseRevision, head.ConfirmedRevision,
			)
		}

		artifact := artifactIdentity
		artifact.FrozenAt = now
		if err := tx.InsertArtifact(StoreArtifactRecord{
			Artifact: artifact, EncodedPayload: appendExactAgentBytes(preparedPayload),
		}); err != nil {
			if errors.Is(err, ErrStoreConflict) {
				return ErrArtifactConflict
			}
			return fmt.Errorf("insert Agent Artifact: %w", err)
		}

		next := current
		next.Revision++
		next.EventCount++
		next.UpdatedAt = now
		next.Artifact = &ref
		nextRecord := currentRecord
		nextRecord.Task = next
		state := ArtifactFrozen
		nextRecord.ArtifactState = &state
		expectedLease := StoreLeaseState{Owner: command.Meta.Grant.Worker, Fence: command.Meta.Grant.Fence}
		changed, err := tx.CompareAndSwapTask(StoreTaskMutation{
			Ref: command.Task,
			Condition: StoreTaskCondition{
				ExpectedRevision: command.Meta.ExpectedRevision,
				ExpectedLease:    &expectedLease,
			},
			Next: nextRecord,
		})
		if err != nil {
			return fmt.Errorf("update Task for frozen Artifact: %w", err)
		}
		if !changed {
			latest, loadErr := loadTaskRecordFromStore(tx, command.Task)
			if loadErr != nil {
				return loadErr
			}
			if leaseErr := requireArtifactCurrentLease(tx, latest, command.Meta.Grant); leaseErr != nil {
				return leaseErr
			}
			return &RevisionConflictError{Expected: command.Meta.ExpectedRevision, Actual: latest.Task.Revision}
		}
		if err := tx.InsertEvent(StoreEventRecord{Event: Event{
			Task: command.Task, Sequence: next.EventCount, Type: EventArtifactFrozen,
			State: TaskWorking, Revision: next.Revision, Artifact: ref.ID, OccurredAt: now,
		}}); err != nil {
			return fmt.Errorf("append Agent event: %w", err)
		}
		frozen = FreezeArtifactResult{Task: next, Artifact: artifact}
		if err := recordJSONCommandToStore(
			tx, ref.Workspace.Authority, command.Meta.ID, "freeze_artifact",
			digest, "freeze_artifact", frozen, now,
		); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return FreezeArtifactResult{}, err
	}
	if updateReplayed {
		if _, err := agent.validateStoredValueShape(replayRecord.EncodedPayload, maxStoredArtifactBytes); err != nil {
			return FreezeArtifactResult{}, err
		}
	}
	return cloneFreezeResult(frozen), nil
}

func (agent *Agent) GetArtifact(ctx context.Context, ref ArtifactRef) (ArtifactContent, error) {
	if err := validateCallContext(ctx); err != nil {
		return ArtifactContent{}, err
	}
	if err := validateArtifactRef(ref); err != nil {
		return ArtifactContent{}, err
	}
	store, release, err := agent.acquireStore()
	if err != nil {
		return ArtifactContent{}, err
	}
	defer release()
	var record StoreArtifactRecord
	err = store.View(ctx, func(view StoreView) error {
		loaded, err := loadArtifactRecord(view, ref)
		if err != nil {
			return err
		}
		record = loaded
		return nil
	})
	if err != nil {
		return ArtifactContent{}, err
	}
	content, err := agent.artifactContentFromStoreRecord(ctx, record)
	if err != nil {
		return ArtifactContent{}, err
	}
	return cloneArtifactContent(content), nil
}

// RecordApplyReceipt records the caller-side durable decision. It never
// invokes filesystem code. Success advances the confirmed Workspace head only
// when the exact frozen base still matches in the same Store transaction.
func (agent *Agent) RecordApplyReceipt(ctx context.Context, command RecordApplyReceiptCommand) (ApplyReceipt, error) {
	if err := validateCallContext(ctx); err != nil {
		return ApplyReceipt{}, err
	}
	if err := validateReceiptCommand(command); err != nil {
		return ApplyReceipt{}, err
	}
	digest, err := commandDigest("record_apply_receipt", command)
	if err != nil {
		return ApplyReceipt{}, err
	}
	store, release, err := agent.acquireStore()
	if err != nil {
		return ApplyReceipt{}, err
	}
	defer release()

	var receipt ApplyReceipt
	err = store.Update(ctx, func(tx StoreTx) error {
		var replay ApplyReceipt
		if found, err := replayJSONCommandFromStore(
			tx, command.Artifact.Workspace.Authority, command.CommandID,
			"record_apply_receipt", digest, "apply_receipt", &replay,
		); err != nil {
			return err
		} else if found {
			if err := validateStoredReceipt(replay); err != nil {
				return err
			}
			if replay.Artifact != command.Artifact || replay.ID != command.Receipt {
				return fmt.Errorf("%w: receipt command result identity mismatch", ErrCorruptStore)
			}
			authoritative, exists, err := loadVerifiedApplyReceipt(tx, replay.Artifact)
			if err != nil {
				return err
			}
			if !exists || !sameReceipt(replay, authoritative) {
				return fmt.Errorf("%w: receipt command result does not match durable receipt", ErrCorruptStore)
			}
			receipt = replay
			return nil
		}

		record, err := loadArtifactRecord(tx, command.Artifact)
		if err != nil {
			return err
		}
		artifact := record.Artifact
		if artifact.State != ArtifactPublished {
			return fmt.Errorf("%w: receipt requires a published Artifact", ErrArtifactState)
		}
		if command.ArtifactDigest != artifact.Digest || command.BaseRevision != artifact.BaseRevision ||
			command.ResultRevision != artifact.ResultRevision {
			return fmt.Errorf("%w: receipt does not identify the exact published Artifact", ErrReceiptConflict)
		}
		existing, found, err := loadVerifiedApplyReceipt(tx, command.Artifact)
		if err != nil {
			return err
		}
		now, err := checkedClockTime(agent.now)
		if err != nil {
			return err
		}
		now = timestampAtLeast(now, artifact.FrozenAt, *artifact.PublishedAt)
		receipt = ApplyReceipt{
			ID: command.Receipt, Artifact: command.Artifact,
			ArtifactDigest: command.ArtifactDigest, BaseRevision: command.BaseRevision,
			ResultRevision: command.ResultRevision, Decision: command.Decision,
			ObservedRevision: command.ObservedRevision, Code: command.Code,
			Message: command.Message, RecordedAt: now,
		}
		if found {
			if !sameReceiptDecision(existing, receipt) {
				return ErrReceiptConflict
			}
			receipt = existing
		} else {
			if command.Decision == workspace.ApplySuccess {
				changed, err := tx.CompareAndSwapWorkspaceHead(StoreWorkspaceHeadMutation{
					Workspace:        artifact.Ref.Workspace,
					ExpectedRevision: artifact.BaseRevision,
					Next: StoreWorkspaceHeadRecord{Head: WorkspaceHead{
						Workspace: artifact.Ref.Workspace, ConfirmedRevision: artifact.ResultRevision, UpdatedAt: now,
					}},
				})
				if err != nil {
					return fmt.Errorf("advance Agent Workspace head: %w", err)
				}
				if !changed {
					return fmt.Errorf("%w: success receipt base is no longer confirmed", ErrWorkspaceConflict)
				}
			}
			if err := tx.InsertApplyReceipt(StoreApplyReceiptRecord{Receipt: receipt}); err != nil {
				if errors.Is(err, ErrStoreConflict) {
					return ErrReceiptConflict
				}
				return fmt.Errorf("insert Agent apply receipt: %w", err)
			}
		}
		return recordJSONCommandToStore(
			tx, command.Artifact.Workspace.Authority, command.CommandID,
			"record_apply_receipt", digest, "apply_receipt", receipt, now,
		)
	})
	if err != nil {
		return ApplyReceipt{}, err
	}
	return receipt, nil
}

func (agent *Agent) GetApplyReceipt(ctx context.Context, ref ArtifactRef) (ApplyReceipt, error) {
	if err := validateCallContext(ctx); err != nil {
		return ApplyReceipt{}, err
	}
	if err := validateArtifactRef(ref); err != nil {
		return ApplyReceipt{}, err
	}
	store, release, err := agent.acquireStore()
	if err != nil {
		return ApplyReceipt{}, err
	}
	defer release()
	var receipt ApplyReceipt
	var found bool
	err = store.View(ctx, func(view StoreView) error {
		var err error
		receipt, found, err = loadVerifiedApplyReceipt(view, ref)
		return err
	})
	if err != nil {
		return ApplyReceipt{}, err
	}
	if !found {
		return ApplyReceipt{}, ErrReceiptNotFound
	}
	return receipt, nil
}

func (agent *Agent) GetWorkspaceHead(ctx context.Context, ref WorkspaceRef) (WorkspaceHead, error) {
	if err := validateCallContext(ctx); err != nil {
		return WorkspaceHead{}, err
	}
	if err := validateWorkspaceRef(ref); err != nil {
		return WorkspaceHead{}, err
	}
	store, release, err := agent.acquireStore()
	if err != nil {
		return WorkspaceHead{}, err
	}
	defer release()
	var head WorkspaceHead
	err = store.View(ctx, func(view StoreView) error {
		var err error
		head, err = loadArtifactWorkspaceHead(view, ref)
		return err
	})
	if err != nil {
		return WorkspaceHead{}, err
	}
	return head, nil
}

func loadArtifactRecord(view StoreView, ref ArtifactRef) (StoreArtifactRecord, error) {
	record, err := view.LoadArtifact(ref, StoreReadLimit{MaxBytes: maxStoredArtifactBytes})
	if errors.Is(err, ErrStoreRecordNotFound) {
		return StoreArtifactRecord{}, ErrArtifactNotFound
	}
	if errors.Is(err, ErrStoreRecordTooLarge) {
		return StoreArtifactRecord{}, fmt.Errorf("%w: Agent Artifact exceeds the encoded read limit", ErrCorruptStore)
	}
	if err != nil {
		return StoreArtifactRecord{}, fmt.Errorf("load Agent Artifact: %w", err)
	}
	record = cloneStoreArtifactRecord(record)
	if record.Artifact.Ref != ref {
		return StoreArtifactRecord{}, fmt.Errorf("%w: loaded Artifact identity mismatch", ErrCorruptStore)
	}
	if err := validateStoredArtifactMetadata(record.Artifact); err != nil ||
		len(record.EncodedPayload) == 0 || int64(len(record.EncodedPayload)) > maxStoredArtifactBytes {
		return StoreArtifactRecord{}, fmt.Errorf("%w: invalid encoded Artifact", ErrCorruptStore)
	}
	return record, nil
}

func loadApplyReceipt(view StoreView, ref ArtifactRef) (ApplyReceipt, bool, error) {
	record, err := view.LoadApplyReceipt(ref)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return ApplyReceipt{}, false, nil
	}
	if err != nil {
		return ApplyReceipt{}, false, fmt.Errorf("load Agent apply receipt: %w", err)
	}
	receipt := record.Receipt
	if receipt.Artifact != ref {
		return ApplyReceipt{}, false, fmt.Errorf("%w: loaded apply receipt identity mismatch", ErrCorruptStore)
	}
	if err := validateStoredReceipt(receipt); err != nil {
		return ApplyReceipt{}, false, err
	}
	return receipt, true, nil
}

func loadVerifiedApplyReceipt(view StoreView, ref ArtifactRef) (ApplyReceipt, bool, error) {
	receipt, found, err := loadApplyReceipt(view, ref)
	if err != nil || !found {
		return receipt, found, err
	}
	record, err := loadArtifactRecord(view, ref)
	if err != nil {
		return ApplyReceipt{}, false, err
	}
	artifact := record.Artifact
	if artifact.State != ArtifactPublished || artifact.PublishedAt == nil ||
		receipt.ArtifactDigest != artifact.Digest || receipt.BaseRevision != artifact.BaseRevision ||
		receipt.ResultRevision != artifact.ResultRevision || receipt.RecordedAt.Before(*artifact.PublishedAt) {
		return ApplyReceipt{}, false, fmt.Errorf("%w: apply receipt does not match published Artifact", ErrCorruptStore)
	}
	return receipt, true, nil
}

func verifyArtifactLeaseGrantHistory(view StoreView, grant LeaseGrant) (time.Time, error) {
	if err := validateLeaseGrant(grant); err != nil {
		return time.Time{}, err
	}
	taskRecord, err := loadTaskRecordFromStore(view, grant.Task)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return time.Time{}, fmt.Errorf("%w: Agent lease Task is missing", ErrCorruptStore)
		}
		return time.Time{}, err
	}
	record, err := view.LoadLeaseGrant(grant.Task, grant.Fence)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return time.Time{}, fmt.Errorf("%w: Agent lease has no durable grant history", ErrCorruptStore)
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("load Agent lease grant history: %w", err)
	}
	if record.Grant != grant || !validLeaseTimestamp(record.GrantedAt) {
		return time.Time{}, fmt.Errorf("%w: Agent lease differs from durable grant history", ErrCorruptStore)
	}
	if record.GrantedAt.Before(taskRecord.Task.CreatedAt) {
		return time.Time{}, fmt.Errorf("%w: Agent lease predates its Task", ErrCorruptStore)
	}
	return record.GrantedAt, nil
}

func requireArtifactCurrentLease(view StoreView, task StoreTaskRecord, grant LeaseGrant) error {
	if err := validateLeaseGrant(grant); err != nil {
		return err
	}
	if task.Task.Ref != grant.Task {
		return fmt.Errorf("%w: Agent lease Task identity mismatch", ErrCorruptStore)
	}
	if task.Lease.Fence > LeaseFence(math.MaxInt64) || (task.Lease.Fence == 0 && task.Lease.Owner != "") {
		return fmt.Errorf("%w: invalid current Agent lease", ErrCorruptStore)
	}
	if task.Lease.Owner != "" && validateStable("worker id", string(task.Lease.Owner)) != nil {
		return fmt.Errorf("%w: invalid Agent lease owner", ErrCorruptStore)
	}
	latest, err := view.LoadLatestLeaseGrant(grant.Task)
	if errors.Is(err, ErrStoreRecordNotFound) {
		if task.Lease.Fence != 0 {
			return fmt.Errorf("%w: Agent lease fence has no durable grant history", ErrCorruptStore)
		}
		return ErrStaleLease
	}
	if err != nil {
		return fmt.Errorf("load latest Agent lease history: %w", err)
	}
	if validateLeaseGrant(latest.Grant) != nil || latest.Grant.Task != grant.Task || latest.Grant.Fence != task.Lease.Fence ||
		!validLeaseTimestamp(latest.GrantedAt) || latest.GrantedAt.Before(task.Task.CreatedAt) {
		return fmt.Errorf("%w: Agent lease fence differs from durable grant history", ErrCorruptStore)
	}
	if task.Lease.Owner != "" && latest.Grant.Worker != task.Lease.Owner {
		return fmt.Errorf("%w: current Agent lease owner differs from durable grant", ErrCorruptStore)
	}
	if task.Lease.Owner != grant.Worker || task.Lease.Fence != grant.Fence {
		return ErrStaleLease
	}
	if latest.Grant != grant {
		return fmt.Errorf("%w: current Agent lease differs from durable grant", ErrCorruptStore)
	}
	exact, err := view.LoadLeaseGrant(grant.Task, grant.Fence)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return fmt.Errorf("%w: Agent lease has no durable grant history", ErrCorruptStore)
	}
	if err != nil {
		return fmt.Errorf("load Agent lease grant history: %w", err)
	}
	if exact.Grant != latest.Grant || !exact.GrantedAt.Equal(latest.GrantedAt) {
		return fmt.Errorf("%w: latest Agent lease differs from exact grant history", ErrCorruptStore)
	}
	return nil
}

func loadArtifactWorkspaceHead(view StoreView, ref WorkspaceRef) (WorkspaceHead, error) {
	record, err := view.LoadWorkspaceHead(ref)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return WorkspaceHead{}, ErrWorkspaceNotFound
	}
	if err != nil {
		return WorkspaceHead{}, fmt.Errorf("load Agent Workspace head: %w", err)
	}
	head := record.Head
	if head.Workspace != ref || validateRevision("stored confirmed revision", head.ConfirmedRevision) != nil ||
		head.UpdatedAt.IsZero() {
		return WorkspaceHead{}, fmt.Errorf("%w: invalid Workspace head", ErrCorruptStore)
	}
	return head, nil
}

func validateArtifactPayloadShape(payload workspace.Payload) error {
	if payload.MediaType == "" || payload.MediaType != strings.TrimSpace(payload.MediaType) ||
		len(payload.MediaType) > 128 || !utf8.ValidString(payload.MediaType) ||
		strings.ContainsAny(payload.MediaType, "\r\n\x00") {
		return fmt.Errorf("%w: Artifact media type is invalid", ErrInvalidArgument)
	}
	if _, _, err := mime.ParseMediaType(payload.MediaType); err != nil {
		return fmt.Errorf("%w: Artifact media type is invalid: %v", ErrInvalidArgument, err)
	}
	if len(payload.Data) == 0 || int64(len(payload.Data)) > absoluteArtifactMax {
		return fmt.Errorf("%w: Artifact payload must be 1..%d bytes", ErrInvalidArgument, absoluteArtifactMax)
	}
	return nil
}

func validateArtifactRef(ref ArtifactRef) error {
	if err := validateWorkspaceRef(ref.Workspace); err != nil {
		return err
	}
	return validateStable("artifact id", string(ref.ID))
}

func validateRevision(label string, revision workspace.Revision) error {
	value := string(revision)
	if value == "" || len(value) > maxRevisionBytes || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidArgument, label)
	}
	return nil
}

func validateReceiptCommand(command RecordApplyReceiptCommand) error {
	if err := validateStable("command id", string(command.CommandID)); err != nil {
		return err
	}
	if err := validateStable("receipt id", string(command.Receipt)); err != nil {
		return err
	}
	if err := validateArtifactRef(command.Artifact); err != nil {
		return err
	}
	if err := validateDigest("artifact digest", command.ArtifactDigest); err != nil {
		return err
	}
	if err := validateRevision("base revision", command.BaseRevision); err != nil {
		return err
	}
	if err := validateRevision("result revision", command.ResultRevision); err != nil {
		return err
	}
	if !command.Decision.Valid() {
		return fmt.Errorf("%w: apply decision is invalid", ErrInvalidArgument)
	}
	if command.ObservedRevision != "" {
		if err := validateRevision("observed revision", command.ObservedRevision); err != nil {
			return err
		}
	}
	if command.Decision == workspace.ApplySuccess && command.ObservedRevision != command.ResultRevision {
		return fmt.Errorf("%w: success receipt must observe its exact result revision", ErrInvalidArgument)
	}
	if command.Decision == workspace.ApplyConflict && command.ObservedRevision == "" {
		return fmt.Errorf("%w: conflict receipt must include the observed revision", ErrInvalidArgument)
	}
	if len(command.Code) > 128 || !utf8.ValidString(command.Code) || strings.ContainsAny(command.Code, "\x00\r\n") {
		return fmt.Errorf("%w: receipt code is invalid", ErrInvalidArgument)
	}
	if len(command.Message) > 4096 || !utf8.ValidString(command.Message) || strings.ContainsRune(command.Message, '\x00') {
		return fmt.Errorf("%w: receipt message is invalid", ErrInvalidArgument)
	}
	return nil
}

func validateStoredArtifact(content ArtifactContent) error {
	artifact := content.Artifact
	if err := validateStoredArtifactMetadata(artifact); err != nil ||
		artifact.PayloadSize != int64(len(content.Payload.Data)) || artifact.MediaType != content.Payload.MediaType ||
		len(content.Payload.Data) == 0 {
		return fmt.Errorf("%w: invalid Artifact %q", ErrCorruptStore, artifact.Ref.ID)
	}
	if digestPayload(content.Payload) != artifact.PayloadDigest {
		return fmt.Errorf("%w: Artifact payload digest mismatch for %q", ErrCorruptStore, artifact.Ref.ID)
	}
	return nil
}

func validateStoredArtifactMetadata(artifact Artifact) error {
	if err := validateArtifactRef(artifact.Ref); err != nil || validateTaskRef(artifact.Task) != nil ||
		artifact.Task.Workspace != artifact.Ref.Workspace || artifact.PayloadSize <= 0 ||
		artifact.PayloadSize > absoluteArtifactMax || artifact.FrozenAt.IsZero() {
		return fmt.Errorf("%w: invalid Artifact %q", ErrCorruptStore, artifact.Ref.ID)
	}
	if validateRevision("base revision", artifact.BaseRevision) != nil ||
		validateRevision("result revision", artifact.ResultRevision) != nil ||
		validateDigest("artifact digest", artifact.Digest) != nil ||
		validateDigest("payload digest", artifact.PayloadDigest) != nil ||
		artifact.BaseRevision == artifact.ResultRevision {
		return fmt.Errorf("%w: invalid Artifact identity %q", ErrCorruptStore, artifact.Ref.ID)
	}
	switch artifact.State {
	case ArtifactFrozen:
		if artifact.PublishedAt != nil || artifact.DiscardedAt != nil {
			return fmt.Errorf("%w: invalid frozen Artifact timestamps", ErrCorruptStore)
		}
	case ArtifactPublished:
		if artifact.PublishedAt == nil || artifact.DiscardedAt != nil || artifact.PublishedAt.Before(artifact.FrozenAt) {
			return fmt.Errorf("%w: invalid published Artifact timestamps", ErrCorruptStore)
		}
	case ArtifactDiscarded:
		if artifact.PublishedAt != nil || artifact.DiscardedAt == nil || artifact.DiscardedAt.Before(artifact.FrozenAt) {
			return fmt.Errorf("%w: invalid discarded Artifact timestamps", ErrCorruptStore)
		}
	default:
		return fmt.Errorf("%w: invalid Artifact state", ErrCorruptStore)
	}
	if nextRevision(artifact.BaseRevision, artifact.Ref, artifact.PayloadDigest) != artifact.ResultRevision ||
		digestArtifact(
			artifact.Ref, artifact.Task, artifact.BaseRevision, artifact.ResultRevision,
			artifact.PayloadDigest, artifact.MediaType,
		) != artifact.Digest {
		return fmt.Errorf("%w: Artifact digest mismatch for %q", ErrCorruptStore, artifact.Ref.ID)
	}
	return nil
}

func validateStoredReceipt(receipt ApplyReceipt) error {
	command := RecordApplyReceiptCommand{
		CommandID: "stored-receipt", Receipt: receipt.ID, Artifact: receipt.Artifact,
		ArtifactDigest: receipt.ArtifactDigest, BaseRevision: receipt.BaseRevision,
		ResultRevision: receipt.ResultRevision, Decision: receipt.Decision,
		ObservedRevision: receipt.ObservedRevision, Code: receipt.Code, Message: receipt.Message,
	}
	if err := validateReceiptCommand(command); err != nil || receipt.RecordedAt.IsZero() {
		return fmt.Errorf("%w: invalid apply receipt %q", ErrCorruptStore, receipt.ID)
	}
	return nil
}

func validateFreezeResult(result FreezeArtifactResult) error {
	if err := validateStoredTask(result.Task); err != nil || result.Task.Artifact == nil ||
		*result.Task.Artifact != result.Artifact.Ref || result.Artifact.State != ArtifactFrozen ||
		result.Artifact.Task != result.Task.Ref || result.Artifact.PayloadSize <= 0 ||
		result.Artifact.FrozenAt.IsZero() || !result.Artifact.FrozenAt.Equal(result.Task.UpdatedAt) ||
		result.Artifact.PublishedAt != nil || result.Artifact.DiscardedAt != nil ||
		validateRevision("base revision", result.Artifact.BaseRevision) != nil ||
		validateRevision("result revision", result.Artifact.ResultRevision) != nil ||
		validateDigest("artifact digest", result.Artifact.Digest) != nil ||
		validateDigest("payload digest", result.Artifact.PayloadDigest) != nil {
		return fmt.Errorf("%w: invalid frozen Artifact command result", ErrCorruptStore)
	}
	return nil
}

func validateDigest(label string, digest workspace.Digest) error {
	value := string(digest)
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidArgument, label)
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:")); err != nil {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidArgument, label)
	}
	return nil
}

func digestPayload(payload workspace.Payload) workspace.Digest {
	encoded, _ := json.Marshal(payload)
	return sha256Identity(encoded)
}

func nextRevision(base workspace.Revision, ref ArtifactRef, payload workspace.Digest) workspace.Revision {
	encoded, _ := json.Marshal(struct {
		Base     workspace.Revision `json:"base"`
		Surface  string             `json:"surface"`
		Artifact ArtifactRef        `json:"artifact"`
		Payload  workspace.Digest   `json:"payload"`
	}{Base: base, Surface: "agent", Artifact: ref, Payload: payload})
	return workspace.Revision(sha256Identity(encoded))
}

func digestArtifact(
	ref ArtifactRef,
	task TaskRef,
	base, result workspace.Revision,
	payload workspace.Digest,
	mediaType string,
) workspace.Digest {
	encoded, _ := json.Marshal(struct {
		Ref       ArtifactRef        `json:"ref"`
		Task      TaskRef            `json:"task"`
		Base      workspace.Revision `json:"base"`
		Result    workspace.Revision `json:"result"`
		Payload   workspace.Digest   `json:"payload"`
		MediaType string             `json:"media_type"`
	}{ref, task, base, result, payload, mediaType})
	return sha256Identity(encoded)
}

func sha256Identity(payload []byte) workspace.Digest {
	sum := sha256.Sum256(payload)
	return workspace.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

func sameReceiptDecision(left, right ApplyReceipt) bool {
	return left.ID == right.ID && left.Artifact == right.Artifact &&
		left.ArtifactDigest == right.ArtifactDigest && left.BaseRevision == right.BaseRevision &&
		left.ResultRevision == right.ResultRevision && left.Decision == right.Decision &&
		left.ObservedRevision == right.ObservedRevision && left.Code == right.Code && left.Message == right.Message
}

func sameReceipt(left, right ApplyReceipt) bool {
	return sameReceiptDecision(left, right) && left.RecordedAt.Equal(right.RecordedAt)
}

func sameArtifactIdentity(left, right Artifact) bool {
	return left.Ref == right.Ref && left.Task == right.Task &&
		left.BaseRevision == right.BaseRevision && left.ResultRevision == right.ResultRevision &&
		left.Digest == right.Digest && left.PayloadDigest == right.PayloadDigest &&
		left.PayloadSize == right.PayloadSize && left.MediaType == right.MediaType &&
		left.FrozenAt.Equal(right.FrozenAt)
}

func cloneArtifactContent(content ArtifactContent) ArtifactContent {
	content.Payload.Data = append([]byte(nil), content.Payload.Data...)
	if content.Artifact.PublishedAt != nil {
		value := *content.Artifact.PublishedAt
		content.Artifact.PublishedAt = &value
	}
	if content.Artifact.DiscardedAt != nil {
		value := *content.Artifact.DiscardedAt
		content.Artifact.DiscardedAt = &value
	}
	return content
}

func cloneFreezeResult(result FreezeArtifactResult) FreezeArtifactResult {
	result.Task = cloneTask(result.Task)
	if result.Artifact.PublishedAt != nil {
		value := *result.Artifact.PublishedAt
		result.Artifact.PublishedAt = &value
	}
	if result.Artifact.DiscardedAt != nil {
		value := *result.Artifact.DiscardedAt
		result.Artifact.DiscardedAt = &value
	}
	return result
}
