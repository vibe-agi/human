package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func loadTaskFromStore(view StoreView, ref TaskRef) (Task, error) {
	record, err := loadTaskRecordFromStore(view, ref)
	if err != nil {
		return Task{}, err
	}
	return cloneTask(record.Task), nil
}

func loadTaskRecordFromStore(view StoreView, ref TaskRef) (StoreTaskRecord, error) {
	record, err := view.LoadTask(ref)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return StoreTaskRecord{}, ErrNotFound
	}
	if err != nil {
		return StoreTaskRecord{}, fmt.Errorf("load Agent task: %w", err)
	}
	if record.Task.Ref != ref {
		return StoreTaskRecord{}, fmt.Errorf("%w: Store returned a different Task identity", ErrCorruptStore)
	}
	if err := validateStoreTaskRecord(record); err != nil {
		return StoreTaskRecord{}, err
	}
	record.Task = cloneTask(record.Task)
	if record.ArtifactState != nil {
		state := *record.ArtifactState
		record.ArtifactState = &state
	}
	return record, nil
}

func validateStoreTaskRecord(record StoreTaskRecord) error {
	if err := validateStoredTask(record.Task); err != nil {
		return err
	}
	if record.Lease.Owner == "" {
		// Fence is deliberately retained after a lease is retired.
	} else if record.Lease.Fence == 0 || record.Task.State.Terminal() {
		return fmt.Errorf("%w: invalid current lease for task %q", ErrCorruptStore, record.Task.Ref.ID)
	}
	if (record.Task.Artifact == nil) != (record.ArtifactState == nil) {
		return fmt.Errorf("%w: partial Artifact state for task %q", ErrCorruptStore, record.Task.Ref.ID)
	}
	if record.ArtifactState != nil {
		state := *record.ArtifactState
		valid := (state == ArtifactFrozen && (record.Task.State == TaskWorking || record.Task.State == TaskInputRequired)) ||
			(state == ArtifactPublished && record.Task.State == TaskCompleted) ||
			(state == ArtifactDiscarded && (record.Task.State == TaskCanceled || record.Task.State == TaskFailed))
		if !valid {
			return fmt.Errorf(
				"%w: Artifact state %q does not match task %q state %q",
				ErrCorruptStore, state, record.Task.Ref.ID, record.Task.State,
			)
		}
	}
	return nil
}

func replayTaskCommandFromStore(
	view StoreView,
	authority AuthorityID,
	id CommandID,
	kind, digest string,
) (Task, bool, error) {
	var task Task
	found, err := replayJSONCommandFromStore(
		view, authority, id, kind, digest, "task", &task,
	)
	if err != nil || !found {
		return Task{}, found, err
	}
	if err := validateStoredTask(task); err != nil {
		return Task{}, false, fmt.Errorf("decode Agent command result: %w", err)
	}
	if task.Ref.Workspace.Authority != authority {
		return Task{}, false, fmt.Errorf("%w: command result authority mismatch", ErrCorruptStore)
	}
	return cloneTask(task), true, nil
}

func replayJSONCommandFromStore(
	view StoreView,
	authority AuthorityID,
	id CommandID,
	kind, digest, resultKind string,
	destination any,
) (bool, error) {
	record, err := view.LookupCommand(
		StoreCommandKey{Authority: authority, ID: id},
		StoreReadLimit{MaxBytes: maxPageBytes},
	)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup Agent command: %w", err)
	}
	if err := validateStoreCommandRecord(record, authority, id); err != nil {
		return false, err
	}
	if record.Kind != kind || string(record.IntentDigest) != digest || record.ResultKind != resultKind {
		return false, ErrIdempotencyConflict
	}
	if err := json.Unmarshal(record.Result, destination); err != nil {
		return false, fmt.Errorf("%w: decode Agent command result: %v", ErrCorruptStore, err)
	}
	return true, nil
}

func validateStoreCommandRecord(record StoreCommandRecord, authority AuthorityID, id CommandID) error {
	if record.Authority != authority || record.ID != id ||
		validateStable("stored command kind", record.Kind) != nil ||
		record.IntentDigest == "" || validateStable("stored command result kind", record.ResultKind) != nil ||
		len(record.Result) == 0 || record.CreatedAt.IsZero() {
		return fmt.Errorf("%w: invalid Agent command record", ErrCorruptStore)
	}
	if byteDigest(record.Result) != string(record.ResultDigest) {
		return fmt.Errorf("%w: Agent command result digest mismatch", ErrCorruptStore)
	}
	return nil
}

func recordTaskCommandToStore(
	tx StoreTx,
	authority AuthorityID,
	id CommandID,
	kind, digest string,
	task Task,
	now time.Time,
) error {
	return recordJSONCommandToStore(tx, authority, id, kind, digest, "task", task, now)
}

func recordJSONCommandToStore(
	tx StoreTx,
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
	record := StoreCommandRecord{
		Authority: authority, ID: id, Kind: kind, IntentDigest: StoreDigest(digest),
		ResultKind: resultKind, Result: encoded, ResultDigest: StoreDigest(byteDigest(encoded)),
		CreatedAt: now,
	}
	if err := tx.InsertCommand(record); err != nil {
		if errors.Is(err, ErrStoreConflict) {
			return ErrIdempotencyConflict
		}
		return fmt.Errorf("record Agent command: %w", err)
	}
	return nil
}

func insertMessageToStore(tx StoreTx, message Message) error {
	parts, err := json.Marshal(message.Parts)
	if err != nil {
		return fmt.Errorf("encode Agent message: %w", err)
	}
	digest, err := contentDigest(message.Parts)
	if err != nil {
		return err
	}
	if err := tx.InsertMessage(StoreMessageRecord{
		ID: message.ID, Task: message.Task, Sequence: message.Sequence,
		Author: message.Author, EncodedParts: parts, PartsDigest: StoreDigest(digest),
		CreatedAt: message.CreatedAt,
	}); err != nil {
		if errors.Is(err, ErrStoreConflict) {
			return ErrMessageConflict
		}
		return fmt.Errorf("insert Agent message: %w", err)
	}
	return nil
}

func messageFromStoreRecord(record StoreMessageRecord) (Message, error) {
	message := Message{
		ID: record.ID, Task: record.Task, Sequence: record.Sequence,
		Author: record.Author, CreatedAt: record.CreatedAt,
	}
	if len(record.EncodedParts) == 0 {
		return Message{}, fmt.Errorf("%w: Agent message %q has empty encoded content", ErrCorruptStore, record.ID)
	}
	if err := json.Unmarshal(record.EncodedParts, &message.Parts); err != nil {
		return Message{}, fmt.Errorf(
			"%w: decode Agent message %q: %v", ErrCorruptStore, record.ID, err,
		)
	}
	actual, err := contentDigest(message.Parts)
	if err != nil || StoreDigest(actual) != record.PartsDigest {
		return Message{}, fmt.Errorf(
			"%w: Agent message %q content digest mismatch", ErrCorruptStore, record.ID,
		)
	}
	if err := validateStoredMessage(message); err != nil {
		return Message{}, err
	}
	return cloneMessage(message), nil
}

func insertEventToStore(tx StoreTx, event Event) error {
	if err := tx.InsertEvent(StoreEventRecord{Event: event}); err != nil {
		return fmt.Errorf("append Agent event: %w", err)
	}
	return nil
}

func verifyLeaseGrantHistoryFromStore(view StoreView, grant LeaseGrant) (time.Time, error) {
	if err := validateLeaseGrant(grant); err != nil {
		return time.Time{}, err
	}
	record, err := view.LoadLeaseGrant(grant.Task, grant.Fence)
	if errors.Is(err, ErrStoreRecordNotFound) {
		return time.Time{}, ErrStaleLease
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("load Agent lease grant history: %w", err)
	}
	if record.Grant != grant || record.GrantedAt.IsZero() {
		return time.Time{}, fmt.Errorf("%w: Agent lease grant history mismatch", ErrCorruptStore)
	}
	return record.GrantedAt, nil
}

func requireCurrentLeaseFromStore(view StoreView, grant LeaseGrant) error {
	if _, err := verifyLeaseGrantHistoryFromStore(view, grant); err != nil {
		return err
	}
	record, err := loadTaskRecordFromStore(view, grant.Task)
	if err != nil {
		return err
	}
	if record.Lease.Owner != grant.Worker || record.Lease.Fence != grant.Fence {
		return ErrStaleLease
	}
	return nil
}

// validateLeaseHistoryHeadFromStore proves that the mutable lease pointer on a
// Task is exactly the head of its immutable grant history. A fenced Task keeps
// the last fence while clearing Owner, so an empty Owner is not equivalent to
// an empty history.
func validateLeaseHistoryHeadFromStore(
	view StoreView,
	record StoreTaskRecord,
) (time.Time, error) {
	latest, err := view.LoadLatestLeaseGrant(record.Task.Ref)
	if errors.Is(err, ErrStoreRecordNotFound) {
		if record.Lease.Fence != 0 || record.Lease.Owner != "" {
			return time.Time{}, fmt.Errorf(
				"%w: Agent lease pointer has no durable grant history", ErrCorruptStore,
			)
		}
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("load latest Agent lease grant: %w", err)
	}
	if err := validateLeaseGrant(latest.Grant); err != nil ||
		latest.Grant.Task != record.Task.Ref ||
		latest.Grant.Fence != record.Lease.Fence ||
		!validLeaseTimestamp(latest.GrantedAt) ||
		latest.GrantedAt.Before(record.Task.CreatedAt) ||
		(record.Lease.Owner != "" && latest.Grant.Worker != record.Lease.Owner) {
		return time.Time{}, fmt.Errorf(
			"%w: Agent lease pointer differs from durable grant history", ErrCorruptStore,
		)
	}
	return latest.GrantedAt, nil
}

func verifyLeaseAssignmentFromStore(view StoreView, assignment LeaseAssignment) error {
	grantedAt, err := verifyLeaseGrantHistoryFromStore(view, assignment.Grant)
	if errors.Is(err, ErrStaleLease) {
		return fmt.Errorf("%w: replayed Agent lease has no durable grant history", ErrCorruptStore)
	}
	if err != nil {
		return err
	}
	if !grantedAt.Equal(assignment.GrantedAt) {
		return fmt.Errorf("%w: replayed Agent lease differs from history", ErrCorruptStore)
	}
	return nil
}

func loadLeaseAssignmentFromStore(view StoreView, ref TaskRef) (LeaseAssignment, error) {
	record, err := loadTaskRecordFromStore(view, ref)
	if err != nil {
		return LeaseAssignment{}, err
	}
	grantedAt, err := validateLeaseHistoryHeadFromStore(view, record)
	if err != nil {
		return LeaseAssignment{}, err
	}
	if record.Lease.Owner == "" {
		return LeaseAssignment{}, ErrLeaseNotFound
	}
	assignment := LeaseAssignment{
		Grant: LeaseGrant{
			Task: ref, Worker: record.Lease.Owner, Fence: record.Lease.Fence,
		},
		Task: cloneTask(record.Task), GrantedAt: grantedAt,
	}
	if err := validateLeaseAssignment(assignment); err != nil {
		return LeaseAssignment{}, err
	}
	return assignment, nil
}
