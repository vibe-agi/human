package agent

import (
	"context"
	"crypto/sha256"
	"database/sql"
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
	database, release, err := agent.acquire()
	if err != nil {
		return FreezeArtifactResult{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return FreezeArtifactResult{}, fmt.Errorf("begin freeze Agent Artifact: %w", err)
	}
	defer tx.Rollback()
	var replay FreezeArtifactResult
	if found, err := replayJSONCommand(
		ctx, tx, command.Task.Workspace.Authority, command.Meta.ID,
		"freeze_artifact", digest, "freeze_artifact", &replay,
	); err != nil {
		return FreezeArtifactResult{}, err
	} else if found {
		if err := validateFreezeResult(replay); err != nil {
			return FreezeArtifactResult{}, err
		}
		wantRef := ArtifactRef{Workspace: command.Task.Workspace, ID: command.Artifact}
		if replay.Task.Ref != command.Task || replay.Artifact.Ref != wantRef {
			return FreezeArtifactResult{}, fmt.Errorf("%w: frozen command result identity mismatch", ErrCorruptStore)
		}
		if _, err := verifyLeaseGrantHistory(ctx, tx, command.Meta.Grant); err != nil {
			return FreezeArtifactResult{}, err
		}
		stored, err := loadArtifactContent(ctx, tx, replay.Artifact.Ref)
		if err != nil || !sameArtifactIdentity(stored.Artifact, replay.Artifact) {
			return FreezeArtifactResult{}, fmt.Errorf("%w: frozen command result does not match Artifact", ErrCorruptStore)
		}
		return cloneFreezeResult(replay), nil
	}
	if int64(len(command.Payload.Data)) > agent.maxArtifactBytes {
		return FreezeArtifactResult{}, fmt.Errorf(
			"%w: Artifact payload must be 1..%d bytes", ErrInvalidArgument, agent.maxArtifactBytes,
		)
	}

	current, err := loadTask(ctx, tx, command.Task)
	if err != nil {
		return FreezeArtifactResult{}, err
	}
	if err := requireCurrentLease(ctx, tx, command.Meta.Grant); err != nil {
		return FreezeArtifactResult{}, err
	}
	if current.Revision != command.Meta.ExpectedRevision {
		return FreezeArtifactResult{}, &RevisionConflictError{
			Expected: command.Meta.ExpectedRevision, Actual: current.Revision,
		}
	}
	if current.State.Terminal() {
		return FreezeArtifactResult{}, &TransitionError{Operation: "freeze_artifact", State: current.State, Terminal: true}
	}
	if current.State != TaskWorking {
		return FreezeArtifactResult{}, &TransitionError{Operation: "freeze_artifact", State: current.State}
	}
	if current.Artifact != nil || current.Submission != nil {
		return FreezeArtifactResult{}, ErrArtifactConflict
	}
	if current.Revision >= math.MaxInt64 || current.EventCount >= math.MaxInt64 {
		return FreezeArtifactResult{}, fmt.Errorf("%w: task counters exhausted SQLite integer range", ErrRevisionConflict)
	}

	now := timestampAtLeast(agent.now(), current.UpdatedAt)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_workspace_heads (
		  authority_id, workspace_id, confirmed_revision, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(authority_id, workspace_id) DO NOTHING`,
		command.Task.Workspace.Authority, command.Task.Workspace.ID,
		command.ExpectedBaseRevision, unixNano(now), unixNano(now),
	); err != nil {
		return FreezeArtifactResult{}, fmt.Errorf("initialize Agent Workspace head: %w", err)
	}
	var confirmed workspace.Revision
	if err := tx.QueryRowContext(ctx, `
		SELECT confirmed_revision
		FROM agent_workspace_heads
		WHERE authority_id = ? AND workspace_id = ?`,
		command.Task.Workspace.Authority, command.Task.Workspace.ID,
	).Scan(&confirmed); err != nil {
		return FreezeArtifactResult{}, fmt.Errorf("load Agent Workspace head: %w", err)
	}
	if confirmed != command.ExpectedBaseRevision {
		return FreezeArtifactResult{}, fmt.Errorf(
			"%w: expected %q, confirmed %q", ErrWorkspaceConflict,
			command.ExpectedBaseRevision, confirmed,
		)
	}

	ref := ArtifactRef{Workspace: command.Task.Workspace, ID: command.Artifact}
	payloadDigest := digestPayload(command.Payload)
	resultRevision := nextRevision(command.ExpectedBaseRevision, ref, payloadDigest)
	artifactDigest := digestArtifact(
		ref, command.Task, command.ExpectedBaseRevision, resultRevision,
		payloadDigest, command.Payload.MediaType,
	)
	artifact := Artifact{
		Ref: ref, Task: command.Task, State: ArtifactFrozen,
		BaseRevision: command.ExpectedBaseRevision, ResultRevision: resultRevision,
		Digest: artifactDigest, PayloadDigest: payloadDigest,
		PayloadSize: int64(len(command.Payload.Data)), MediaType: command.Payload.MediaType,
		FrozenAt: now,
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_artifacts (
		  authority_id, workspace_id, artifact_id, task_id, state,
		  base_revision, result_revision, artifact_digest, payload_digest,
		  media_type, payload, payload_size, frozen_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID, command.Task.ID,
		artifact.State, artifact.BaseRevision, artifact.ResultRevision,
		artifact.Digest, artifact.PayloadDigest, artifact.MediaType,
		command.Payload.Data, artifact.PayloadSize, unixNano(now),
	); err != nil {
		if uniqueConstraint(err) {
			return FreezeArtifactResult{}, ErrArtifactConflict
		}
		return FreezeArtifactResult{}, fmt.Errorf("insert Agent Artifact: %w", err)
	}

	next := current
	next.Revision++
	next.EventCount++
	next.UpdatedAt = now
	next.Artifact = &ref
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_tasks
		SET revision = ?, event_count = ?, updated_at = ?
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND revision = ?
		  AND lease_owner = ? AND lease_fence = ?`,
		next.Revision, next.EventCount, unixNano(now), command.Task.Workspace.Authority,
		command.Task.Workspace.ID, command.Task.ID, command.Meta.ExpectedRevision,
		command.Meta.Grant.Worker, command.Meta.Grant.Fence,
	)
	if err != nil {
		return FreezeArtifactResult{}, fmt.Errorf("update Task for frozen Artifact: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return FreezeArtifactResult{}, fmt.Errorf("inspect frozen Artifact Task update: %w", err)
	}
	if affected != 1 {
		if leaseErr := requireCurrentLease(ctx, tx, command.Meta.Grant); leaseErr != nil {
			return FreezeArtifactResult{}, leaseErr
		}
		return FreezeArtifactResult{}, &RevisionConflictError{Expected: command.Meta.ExpectedRevision, Actual: current.Revision}
	}
	if err := insertEvent(ctx, tx, Event{
		Task: command.Task, Sequence: next.EventCount, Type: EventArtifactFrozen,
		State: TaskWorking, Revision: next.Revision, Artifact: ref.ID, OccurredAt: now,
	}); err != nil {
		return FreezeArtifactResult{}, err
	}
	frozen := FreezeArtifactResult{Task: next, Artifact: artifact}
	if err := recordJSONCommand(
		ctx, tx, ref.Workspace.Authority, command.Meta.ID, "freeze_artifact",
		digest, "freeze_artifact", frozen, now,
	); err != nil {
		return FreezeArtifactResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return FreezeArtifactResult{}, fmt.Errorf("commit frozen Agent Artifact: %w", err)
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
	database, release, err := agent.acquire()
	if err != nil {
		return ArtifactContent{}, err
	}
	defer release()
	content, err := loadArtifactContent(ctx, database, ref)
	if err != nil {
		return ArtifactContent{}, err
	}
	return cloneArtifactContent(content), nil
}

// RecordApplyReceipt records the caller-side durable decision. It never
// invokes filesystem code. Success advances the confirmed Workspace head only
// when the exact frozen base still matches in the same SQLite transaction.
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
	database, release, err := agent.acquire()
	if err != nil {
		return ApplyReceipt{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return ApplyReceipt{}, fmt.Errorf("begin Agent apply receipt: %w", err)
	}
	defer tx.Rollback()
	var replay ApplyReceipt
	if found, err := replayJSONCommand(
		ctx, tx, command.Artifact.Workspace.Authority, command.CommandID,
		"record_apply_receipt", digest, "apply_receipt", &replay,
	); err != nil {
		return ApplyReceipt{}, err
	} else if found {
		if err := validateStoredReceipt(replay); err != nil {
			return ApplyReceipt{}, err
		}
		if replay.Artifact != command.Artifact || replay.ID != command.Receipt {
			return ApplyReceipt{}, fmt.Errorf("%w: receipt command result identity mismatch", ErrCorruptStore)
		}
		authoritative, exists, err := loadVerifiedApplyReceipt(ctx, tx, replay.Artifact)
		if err != nil {
			return ApplyReceipt{}, err
		}
		if !exists || !sameReceipt(replay, authoritative) {
			return ApplyReceipt{}, fmt.Errorf("%w: receipt command result does not match durable receipt", ErrCorruptStore)
		}
		return replay, nil
	}

	content, err := loadArtifactContent(ctx, tx, command.Artifact)
	if err != nil {
		return ApplyReceipt{}, err
	}
	artifact := content.Artifact
	if artifact.State != ArtifactPublished {
		return ApplyReceipt{}, fmt.Errorf("%w: receipt requires a published Artifact", ErrArtifactState)
	}
	if command.ArtifactDigest != artifact.Digest || command.BaseRevision != artifact.BaseRevision ||
		command.ResultRevision != artifact.ResultRevision {
		return ApplyReceipt{}, fmt.Errorf("%w: receipt does not identify the exact published Artifact", ErrReceiptConflict)
	}
	existing, found, err := loadVerifiedApplyReceipt(ctx, tx, command.Artifact)
	if err != nil {
		return ApplyReceipt{}, err
	}
	now := timestampAtLeast(agent.now(), artifact.FrozenAt, *artifact.PublishedAt)
	receipt := ApplyReceipt{
		ID: command.Receipt, Artifact: command.Artifact,
		ArtifactDigest: command.ArtifactDigest, BaseRevision: command.BaseRevision,
		ResultRevision: command.ResultRevision, Decision: command.Decision,
		ObservedRevision: command.ObservedRevision, Code: command.Code,
		Message: command.Message, RecordedAt: now,
	}
	if found {
		if !sameReceiptDecision(existing, receipt) {
			return ApplyReceipt{}, ErrReceiptConflict
		}
		receipt = existing
	} else {
		if command.Decision == workspace.ApplySuccess {
			result, err := tx.ExecContext(ctx, `
				UPDATE agent_workspace_heads
				SET confirmed_revision = ?, updated_at = ?
				WHERE authority_id = ? AND workspace_id = ? AND confirmed_revision = ?`,
				artifact.ResultRevision, unixNano(now), artifact.Ref.Workspace.Authority,
				artifact.Ref.Workspace.ID, artifact.BaseRevision,
			)
			if err != nil {
				return ApplyReceipt{}, fmt.Errorf("advance Agent Workspace head: %w", err)
			}
			affected, err := result.RowsAffected()
			if err != nil || affected != 1 {
				return ApplyReceipt{}, fmt.Errorf("%w: success receipt base is no longer confirmed", ErrWorkspaceConflict)
			}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO agent_apply_receipts (
			  authority_id, workspace_id, artifact_id, receipt_id, decision,
			  artifact_digest, base_revision, result_revision, observed_revision,
			  code, message, recorded_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			receipt.Artifact.Workspace.Authority, receipt.Artifact.Workspace.ID,
			receipt.Artifact.ID, receipt.ID, receipt.Decision, receipt.ArtifactDigest,
			receipt.BaseRevision, receipt.ResultRevision, receipt.ObservedRevision,
			receipt.Code, receipt.Message, unixNano(now),
		); err != nil {
			if uniqueConstraint(err) {
				return ApplyReceipt{}, ErrReceiptConflict
			}
			return ApplyReceipt{}, fmt.Errorf("insert Agent apply receipt: %w", err)
		}
	}
	if err := recordJSONCommand(
		ctx, tx, command.Artifact.Workspace.Authority, command.CommandID,
		"record_apply_receipt", digest, "apply_receipt", receipt, now,
	); err != nil {
		return ApplyReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApplyReceipt{}, fmt.Errorf("commit Agent apply receipt: %w", err)
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
	database, release, err := agent.acquire()
	if err != nil {
		return ApplyReceipt{}, err
	}
	defer release()
	receipt, found, err := loadVerifiedApplyReceipt(ctx, database, ref)
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
	database, release, err := agent.acquire()
	if err != nil {
		return WorkspaceHead{}, err
	}
	defer release()
	var head WorkspaceHead
	var updated int64
	head.Workspace = ref
	if err := database.QueryRowContext(ctx, `
		SELECT confirmed_revision, updated_at
		FROM agent_workspace_heads
		WHERE authority_id = ? AND workspace_id = ?`, ref.Authority, ref.ID,
	).Scan(&head.ConfirmedRevision, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceHead{}, ErrWorkspaceNotFound
		}
		return WorkspaceHead{}, fmt.Errorf("load Agent Workspace head: %w", err)
	}
	head.UpdatedAt = fromUnixNano(updated)
	if err := validateRevision("stored confirmed revision", head.ConfirmedRevision); err != nil || head.UpdatedAt.IsZero() {
		return WorkspaceHead{}, fmt.Errorf("%w: invalid Workspace head", ErrCorruptStore)
	}
	return head, nil
}

func loadArtifactContent(ctx context.Context, query queryer, ref ArtifactRef) (ArtifactContent, error) {
	var content ArtifactContent
	var frozen int64
	var published, discarded sql.NullInt64
	content.Artifact.Ref = ref
	if err := query.QueryRowContext(ctx, `
		SELECT task_id, state, base_revision, result_revision, artifact_digest,
		       payload_digest, media_type,
		       CASE WHEN payload_size <= ? AND length(payload) = payload_size
		            THEN payload ELSE NULL END,
		       payload_size,
		       frozen_at, published_at, discarded_at
		FROM agent_artifacts
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		absoluteArtifactMax, ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(
		&content.Artifact.Task.ID, &content.Artifact.State,
		&content.Artifact.BaseRevision, &content.Artifact.ResultRevision,
		&content.Artifact.Digest, &content.Artifact.PayloadDigest,
		&content.Artifact.MediaType, &content.Payload.Data, &content.Artifact.PayloadSize,
		&frozen, &published, &discarded,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ArtifactContent{}, ErrArtifactNotFound
		}
		return ArtifactContent{}, fmt.Errorf("load Agent Artifact: %w", err)
	}
	content.Artifact.Task.Workspace = ref.Workspace
	content.Payload.MediaType = content.Artifact.MediaType
	content.Artifact.FrozenAt = fromUnixNano(frozen)
	if published.Valid {
		value := fromUnixNano(published.Int64)
		content.Artifact.PublishedAt = &value
	}
	if discarded.Valid {
		value := fromUnixNano(discarded.Int64)
		content.Artifact.DiscardedAt = &value
	}
	if err := validateStoredArtifact(content); err != nil {
		return ArtifactContent{}, err
	}
	return content, nil
}

func loadApplyReceipt(ctx context.Context, query queryer, ref ArtifactRef) (ApplyReceipt, bool, error) {
	var receipt ApplyReceipt
	var recorded int64
	receipt.Artifact = ref
	err := query.QueryRowContext(ctx, `
		SELECT receipt_id, decision, artifact_digest, base_revision,
		       result_revision, observed_revision, code, message, recorded_at
		FROM agent_apply_receipts
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(
		&receipt.ID, &receipt.Decision, &receipt.ArtifactDigest,
		&receipt.BaseRevision, &receipt.ResultRevision, &receipt.ObservedRevision,
		&receipt.Code, &receipt.Message, &recorded,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ApplyReceipt{}, false, nil
	}
	if err != nil {
		return ApplyReceipt{}, false, fmt.Errorf("load Agent apply receipt: %w", err)
	}
	receipt.RecordedAt = fromUnixNano(recorded)
	if err := validateStoredReceipt(receipt); err != nil {
		return ApplyReceipt{}, false, err
	}
	return receipt, true, nil
}

func loadVerifiedApplyReceipt(ctx context.Context, query queryer, ref ArtifactRef) (ApplyReceipt, bool, error) {
	receipt, found, err := loadApplyReceipt(ctx, query, ref)
	if err != nil || !found {
		return receipt, found, err
	}
	content, err := loadArtifactContent(ctx, query, ref)
	if err != nil {
		return ApplyReceipt{}, false, err
	}
	artifact := content.Artifact
	if artifact.State != ArtifactPublished || artifact.PublishedAt == nil ||
		receipt.ArtifactDigest != artifact.Digest || receipt.BaseRevision != artifact.BaseRevision ||
		receipt.ResultRevision != artifact.ResultRevision || receipt.RecordedAt.Before(*artifact.PublishedAt) {
		return ApplyReceipt{}, false, fmt.Errorf("%w: apply receipt does not match published Artifact", ErrCorruptStore)
	}
	return receipt, true, nil
}

func replayJSONCommand(
	ctx context.Context,
	tx *sql.Tx,
	authority AuthorityID,
	id CommandID,
	kind, digest, resultKind string,
	result any,
) (bool, error) {
	var storedKind, storedDigest, storedResultKind, storedResultDigest string
	var encoded []byte
	err := tx.QueryRowContext(ctx, `
		SELECT kind, digest, result_kind, result, result_digest
		FROM agent_commands
		WHERE authority_id = ? AND command_id = ?`, authority, id,
	).Scan(&storedKind, &storedDigest, &storedResultKind, &encoded, &storedResultDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup Agent command: %w", err)
	}
	if byteDigest(encoded) != storedResultDigest {
		return false, fmt.Errorf("%w: Agent command result digest mismatch", ErrCorruptStore)
	}
	if storedKind != kind || storedDigest != digest || storedResultKind != resultKind {
		return false, ErrIdempotencyConflict
	}
	if err := json.Unmarshal(encoded, result); err != nil {
		return false, fmt.Errorf("%w: decode Agent command result: %v", ErrCorruptStore, err)
	}
	return true, nil
}

func recordJSONCommand(
	ctx context.Context,
	tx *sql.Tx,
	authority AuthorityID,
	id CommandID,
	kind, digest, resultKind string,
	result any,
	now time.Time,
) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode Agent command result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_commands (
		  authority_id, command_id, kind, digest, result_kind, result, result_digest, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, authority, id, kind, digest,
		resultKind, encoded, byteDigest(encoded), unixNano(now),
	); err != nil {
		if uniqueConstraint(err) {
			return ErrIdempotencyConflict
		}
		return fmt.Errorf("record Agent command: %w", err)
	}
	return nil
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
	if err := validateArtifactRef(artifact.Ref); err != nil || validateTaskRef(artifact.Task) != nil ||
		artifact.Task.Workspace != artifact.Ref.Workspace || artifact.PayloadSize <= 0 ||
		artifact.PayloadSize != int64(len(content.Payload.Data)) || artifact.MediaType != content.Payload.MediaType ||
		artifact.FrozenAt.IsZero() {
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
	if digestPayload(content.Payload) != artifact.PayloadDigest ||
		nextRevision(artifact.BaseRevision, artifact.Ref, artifact.PayloadDigest) != artifact.ResultRevision ||
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
