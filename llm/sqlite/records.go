package sqlite

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/vibe-agi/human/llm"
)

func encodeRecord(kind llm.StoreRecordKind, value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode HumanLLM %s: %w", kind, err)
	}
	return encoded, nil
}

func decodeRecord(kind llm.StoreRecordKind, key string, encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := decoder.Decode(destination); err != nil {
		return &llm.StoreCorruptError{Record: kind, Key: key, Cause: err}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return &llm.StoreCorruptError{Record: kind, Key: key, Cause: err}
	}
	return nil
}

func unixNano(value time.Time) int64 { return value.UnixNano() }

func nullableUnixNano(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UnixNano()
}

func timeFromNullable(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	result := time.Unix(0, value.Int64).UTC()
	return &result
}

func validTaskKey(key llm.StoreTaskKey) bool {
	return key.Caller != "" && key.Task != ""
}

func validRequestKey(key llm.StoreRequestKey) bool {
	return key.Caller != "" && key.IdempotencyKey != ""
}

func validToolKey(key llm.StoreToolExecutionKey) bool {
	return validTaskKey(key.Task) && key.ToolCallID != ""
}

func validateTaskRecord(record llm.StoreTaskRecord) error {
	if !validTaskKey(record.Key) || record.Revision == 0 ||
		record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() || !taskStateValid(record.State) {
		return invalidArgument("invalid task record")
	}
	switch record.CapabilityTier {
	case llm.TierChat:
	case llm.TierRemoteTools, llm.TierWorkspace:
		if record.WorkspaceKey == "" || record.HarnessID == "" ||
			record.HarnessVersion == "" || record.HarnessSessionID == "" {
			return invalidArgument("tool-capable task requires complete affinity")
		}
	default:
		return invalidArgument("invalid capability tier %q", record.CapabilityTier)
	}
	if err := record.Codec.Validate(); err != nil {
		return invalidArgument("invalid task codec: %v", err)
	}
	return nil
}

func validateRequestRecord(record llm.StoreRequestRecord) error {
	if !validRequestKey(record.Key) || !validTaskKey(record.Task) || record.RequestID == "" ||
		record.ResponseID == "" || record.RequestDigest == "" || record.Revision == 0 ||
		record.CreatedAt.IsZero() || (record.Mode != llm.ResponseStream && record.Mode != llm.ResponseAggregate) {
		return invalidArgument("invalid request record")
	}
	if err := record.Codec.Validate(); err != nil {
		return invalidArgument("invalid request codec: %v", err)
	}
	if record.PayloadPrunedAt != nil &&
		(len(record.CanonicalPayload) != 0 || len(record.Decision.Body) != 0) {
		return invalidArgument("tombstoned request retains payload bytes")
	}
	return nil
}

func validateResponseEventRecord(record llm.StoreResponseEventRecord) error {
	if !validRequestKey(record.Request) || record.Sequence == 0 ||
		(record.Kind != llm.StoreEventCheckpoint && record.Kind != llm.StoreEventWire) ||
		record.CreatedAt.IsZero() || ((record.Worker == "") != (record.WorkerEventID == "")) ||
		((record.WorkerEventID == "") != (record.WorkerEventDigest == "")) {
		return invalidArgument("invalid response event record")
	}
	return nil
}

func validateWorkerReceiptRecord(record llm.StoreWorkerReceiptRecord) error {
	if !validRequestKey(record.Request) || record.EventID == "" || record.Worker == "" ||
		record.Digest == "" || record.CreatedAt.IsZero() {
		return invalidArgument("invalid worker receipt record")
	}
	return nil
}

func validateToolRecord(record llm.StoreToolExecutionRecord) error {
	if !validToolKey(record.Key) || record.InputDigest == "" || record.Revision == 0 ||
		record.CreatedAt.IsZero() || (record.State != llm.ToolExecutionPending && record.State != llm.ToolExecutionCompleted) {
		return invalidArgument("invalid tool execution record")
	}
	return nil
}

func taskIdentityEqual(left, right llm.StoreTaskRecord) bool {
	return left.Key == right.Key &&
		left.WorkspaceKey == right.WorkspaceKey &&
		left.CapabilityTier == right.CapabilityTier &&
		left.Codec.Equal(right.Codec) &&
		left.HarnessID == right.HarnessID &&
		left.HarnessVersion == right.HarnessVersion &&
		left.HarnessSessionID == right.HarnessSessionID &&
		left.WorkspaceRoot == right.WorkspaceRoot &&
		left.ExecAllowed == right.ExecAllowed &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func requestIdentityEqual(left, right llm.StoreRequestRecord) bool {
	return left.Key == right.Key && left.Task == right.Task &&
		left.RequestID == right.RequestID && left.ResponseID == right.ResponseID &&
		left.RequestDigest == right.RequestDigest && left.Codec.Equal(right.Codec) &&
		left.Mode == right.Mode && left.CreatedAt.Equal(right.CreatedAt)
}

func toolIdentityEqual(left, right llm.StoreToolExecutionRecord) bool {
	return left.Key == right.Key && left.InputDigest == right.InputDigest &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func (view *view) LoadTask(key llm.StoreTaskKey) (llm.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreTaskRecord{}, err
	}
	if !validTaskKey(key) {
		return llm.StoreTaskRecord{}, invalidArgument("invalid task key")
	}
	return loadTask(view.unit, key)
}

func loadTask(unit *unit, key llm.StoreTaskKey) (llm.StoreTaskRecord, error) {
	var encoded []byte
	err := unit.tx.QueryRowContext(unit.ctx, `
		SELECT record FROM llm_tasks WHERE caller_id = ? AND task_id = ?`,
		key.Caller, key.Task,
	).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreTaskRecord{}, notFound(llm.StoreRecordTask, key)
	}
	if err != nil {
		return llm.StoreTaskRecord{}, fmt.Errorf("load HumanLLM task: %w", err)
	}
	var record llm.StoreTaskRecord
	if err := decodeRecord(llm.StoreRecordTask, fmt.Sprint(key), encoded, &record); err != nil {
		return llm.StoreTaskRecord{}, err
	}
	if record.Key != key {
		return llm.StoreTaskRecord{}, &llm.StoreCorruptError{
			Record: llm.StoreRecordTask, Key: fmt.Sprint(key), Cause: errors.New("task key does not match physical identity"),
		}
	}
	return record, nil
}

func (view *view) FindOpenTask(affinity llm.StoreTaskAffinity) (llm.StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreTaskRecord{}, err
	}
	if affinity.Caller == "" || affinity.WorkspaceKey == "" || affinity.HarnessID == "" ||
		affinity.HarnessVersion == "" || affinity.HarnessSessionID == "" {
		return llm.StoreTaskRecord{}, invalidArgument("invalid task affinity")
	}
	var encoded []byte
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT record FROM llm_tasks
		WHERE caller_id = ? AND workspace_key = ? AND harness_id = ?
		  AND harness_version = ? AND harness_session_id = ?
		  AND capability_tier IN ('remote_tools', 'workspace')
		  AND state NOT IN ('completed', 'rejected', 'expired', 'failed')`,
		affinity.Caller, affinity.WorkspaceKey, affinity.HarnessID,
		affinity.HarnessVersion, affinity.HarnessSessionID,
	).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreTaskRecord{}, notFound(llm.StoreRecordTask, affinity)
	}
	if err != nil {
		return llm.StoreTaskRecord{}, fmt.Errorf("find HumanLLM open task: %w", err)
	}
	var record llm.StoreTaskRecord
	if err := decodeRecord(llm.StoreRecordTask, fmt.Sprint(affinity), encoded, &record); err != nil {
		return llm.StoreTaskRecord{}, err
	}
	return record, nil
}

func (tx *tx) InsertTask(record llm.StoreTaskRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := validateTaskRecord(record); err != nil {
		return err
	}
	if _, err := loadTask(tx.unit, record.Key); err == nil {
		return conflict(llm.StoreConstraintTaskKey, record.Key)
	} else if !errors.Is(err, llm.ErrStoreRecordNotFound) {
		return err
	}
	if !record.State.Terminal() &&
		(record.CapabilityTier == llm.TierRemoteTools || record.CapabilityTier == llm.TierWorkspace) {
		if _, err := tx.FindOpenTask(llm.StoreTaskAffinity{
			Caller: record.Key.Caller, WorkspaceKey: record.WorkspaceKey,
			HarnessID: record.HarnessID, HarnessVersion: record.HarnessVersion,
			HarnessSessionID: record.HarnessSessionID,
		}); err == nil {
			return conflict(llm.StoreConstraintOpenAffinity, record.Key)
		} else if !errors.Is(err, llm.ErrStoreRecordNotFound) {
			return err
		}
	}
	codec, err := encodeRecord(llm.StoreRecordTask, record.Codec)
	if err != nil {
		return err
	}
	encoded, err := encodeRecord(llm.StoreRecordTask, record)
	if err != nil {
		return err
	}
	_, err = tx.unit.tx.ExecContext(tx.unit.ctx, `
		INSERT INTO llm_tasks (
		  caller_id, task_id, workspace_key, capability_tier, codec,
		  harness_id, harness_version, harness_session_id, state, revision,
		  created_at, updated_at, record
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Key.Caller, record.Key.Task, record.WorkspaceKey, record.CapabilityTier, codec,
		record.HarnessID, record.HarnessVersion, record.HarnessSessionID,
		record.State, record.Revision, unixNano(record.CreatedAt), unixNano(record.UpdatedAt), encoded,
	)
	if err != nil {
		return fmt.Errorf("insert HumanLLM task: %w", err)
	}
	return nil
}

func (tx *tx) CompareAndSwapTask(mutation llm.StoreTaskMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if !validTaskKey(mutation.Key) || mutation.ExpectedRevision == 0 || mutation.Next.Revision == 0 {
		return false, invalidArgument("invalid task mutation")
	}
	current, err := loadTask(tx.unit, mutation.Key)
	if err != nil {
		return false, err
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if !taskIdentityEqual(current, mutation.Next) || mutation.Next.Revision != current.Revision+1 {
		return false, conflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	if !mutation.Next.State.Terminal() &&
		(mutation.Next.CapabilityTier == llm.TierRemoteTools || mutation.Next.CapabilityTier == llm.TierWorkspace) {
		found, err := tx.FindOpenTask(llm.StoreTaskAffinity{
			Caller: mutation.Next.Key.Caller, WorkspaceKey: mutation.Next.WorkspaceKey,
			HarnessID: mutation.Next.HarnessID, HarnessVersion: mutation.Next.HarnessVersion,
			HarnessSessionID: mutation.Next.HarnessSessionID,
		})
		if err == nil && found.Key != mutation.Key {
			return false, conflict(llm.StoreConstraintOpenAffinity, mutation.Key)
		}
		if err != nil && !errors.Is(err, llm.ErrStoreRecordNotFound) {
			return false, err
		}
	}
	codec, err := encodeRecord(llm.StoreRecordTask, mutation.Next.Codec)
	if err != nil {
		return false, err
	}
	encoded, err := encodeRecord(llm.StoreRecordTask, mutation.Next)
	if err != nil {
		return false, err
	}
	result, err := tx.unit.tx.ExecContext(tx.unit.ctx, `
		UPDATE llm_tasks SET
		  workspace_key = ?, capability_tier = ?, codec = ?, harness_id = ?,
		  harness_version = ?, harness_session_id = ?, state = ?, revision = ?,
		  updated_at = ?, record = ?
		WHERE caller_id = ? AND task_id = ? AND revision = ?`,
		mutation.Next.WorkspaceKey, mutation.Next.CapabilityTier, codec,
		mutation.Next.HarnessID, mutation.Next.HarnessVersion, mutation.Next.HarnessSessionID,
		mutation.Next.State, mutation.Next.Revision, unixNano(mutation.Next.UpdatedAt), encoded,
		mutation.Key.Caller, mutation.Key.Task, mutation.ExpectedRevision,
	)
	if err != nil {
		return false, fmt.Errorf("compare-and-swap HumanLLM task: %w", err)
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

// TaskStateValidForStore is deliberately not a method on llm.StoreTaskRecord;
// keep adapter vocabulary validation local while the core owns transitions.
func taskStateValid(state llm.TaskState) bool { return state.Valid() }

func (view *view) FindActiveRequest(task llm.StoreTaskKey) (llm.StoreRequestHead, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestHead{}, err
	}
	if !validTaskKey(task) {
		return llm.StoreRequestHead{}, invalidArgument("invalid task key")
	}
	row := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT caller_id, idempotency_key, task_caller_id, task_id,
		       request_id, response_id, request_digest, codec, mode,
		       decision_status, decision_content_type, decision_retry_after,
		       response_complete, recovery_quarantined, last_event_sequence,
		       revision, created_at, completed_at, payload_pruned_at
		FROM llm_requests
		WHERE task_caller_id = ? AND task_id = ? AND response_complete = 0`,
		task.Caller, task.Task,
	)
	return scanRequestHead(row, fmt.Sprint(task))
}

func (view *view) LoadRequestHead(key llm.StoreRequestKey) (llm.StoreRequestHead, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestHead{}, err
	}
	if !validRequestKey(key) {
		return llm.StoreRequestHead{}, invalidArgument("invalid request key")
	}
	row := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT caller_id, idempotency_key, task_caller_id, task_id,
		       request_id, response_id, request_digest, codec, mode,
		       decision_status, decision_content_type, decision_retry_after,
		       response_complete, recovery_quarantined, last_event_sequence,
		       revision, created_at, completed_at, payload_pruned_at
		FROM llm_requests WHERE caller_id = ? AND idempotency_key = ?`,
		key.Caller, key.IdempotencyKey,
	)
	return scanRequestHead(row, fmt.Sprint(key))
}

type rowScanner interface{ Scan(...any) error }

func scanRequestHead(row rowScanner, safeKey string) (llm.StoreRequestHead, error) {
	var head llm.StoreRequestHead
	var codecBytes []byte
	var responseComplete, quarantined int
	var created int64
	var completed, pruned sql.NullInt64
	err := row.Scan(
		&head.Key.Caller, &head.Key.IdempotencyKey,
		&head.Task.Caller, &head.Task.Task,
		&head.RequestID, &head.ResponseID, &head.RequestDigest, &codecBytes, &head.Mode,
		&head.DecisionStatus, &head.DecisionContentType, &head.DecisionRetryAfter,
		&responseComplete, &quarantined, &head.LastEventSequence,
		&head.Revision, &created, &completed, &pruned,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreRequestHead{}, notFound(llm.StoreRecordRequest, safeKey)
	}
	if err != nil {
		return llm.StoreRequestHead{}, fmt.Errorf("load HumanLLM request head: %w", err)
	}
	if err := decodeRecord(llm.StoreRecordRequest, safeKey+"/codec", codecBytes, &head.Codec); err != nil {
		return llm.StoreRequestHead{}, err
	}
	head.ResponseComplete = responseComplete != 0
	head.RecoveryQuarantined = quarantined != 0
	head.CreatedAt = time.Unix(0, created).UTC()
	head.CompletedAt = timeFromNullable(completed)
	head.PayloadPrunedAt = timeFromNullable(pruned)
	return head, nil
}

func (view *view) LoadRequest(
	key llm.StoreRequestKey,
	limit llm.StoreReadLimit,
) (llm.StoreRequestRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	if !validRequestKey(key) {
		return llm.StoreRequestRecord{}, invalidArgument("invalid request key")
	}
	if err := validateReadLimit(llm.StoreRecordRequest, limit); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	var encoded []byte
	var size int64
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT CASE WHEN length(canonical_payload) + length(decision_body) <= ?
		            THEN record ELSE NULL END,
		       length(canonical_payload) + length(decision_body)
		FROM llm_requests WHERE caller_id = ? AND idempotency_key = ?`,
		limit.MaxBytes, key.Caller, key.IdempotencyKey,
	).Scan(&encoded, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreRequestRecord{}, notFound(llm.StoreRecordRequest, key)
	}
	if err != nil {
		return llm.StoreRequestRecord{}, fmt.Errorf("load HumanLLM request: %w", err)
	}
	if size > limit.MaxBytes || encoded == nil {
		return llm.StoreRequestRecord{}, &llm.StoreLimitError{Record: llm.StoreRecordRequest, Limit: limit.MaxBytes}
	}
	var record llm.StoreRequestRecord
	if err := decodeRecord(llm.StoreRecordRequest, fmt.Sprint(key), encoded, &record); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	if record.Key != key {
		return llm.StoreRequestRecord{}, &llm.StoreCorruptError{
			Record: llm.StoreRecordRequest, Key: fmt.Sprint(key), Cause: errors.New("request key does not match physical identity"),
		}
	}
	return record, nil
}

func (view *view) LoadResponseDecision(
	key llm.StoreRequestKey,
	limit llm.StoreReadLimit,
) (llm.StoreResponseDecision, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreResponseDecision{}, err
	}
	if !validRequestKey(key) {
		return llm.StoreResponseDecision{}, invalidArgument("invalid request key")
	}
	if err := validateReadLimit(llm.StoreRecordRequest, limit); err != nil {
		return llm.StoreResponseDecision{}, err
	}
	var decision llm.StoreResponseDecision
	var body []byte
	var size int64
	var bodyIsNil int
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT decision_status, decision_content_type, decision_retry_after,
		       CASE WHEN length(decision_body) <= ? THEN decision_body ELSE NULL END,
		       length(decision_body), decision_body_is_nil
		FROM llm_requests WHERE caller_id = ? AND idempotency_key = ?`,
		limit.MaxBytes, key.Caller, key.IdempotencyKey,
	).Scan(&decision.StatusCode, &decision.ContentType, &decision.RetryAfter, &body, &size, &bodyIsNil)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreResponseDecision{}, notFound(llm.StoreRecordRequest, key)
	}
	if err != nil {
		return llm.StoreResponseDecision{}, fmt.Errorf("load HumanLLM response decision: %w", err)
	}
	if size > limit.MaxBytes {
		return llm.StoreResponseDecision{}, &llm.StoreLimitError{Record: llm.StoreRecordRequest, Limit: limit.MaxBytes}
	}
	if bodyIsNil == 0 {
		decision.Body = make([]byte, len(body))
		copy(decision.Body, body)
	}
	return decision, nil
}

func (tx *tx) InsertRequest(record llm.StoreRequestRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := validateRequestRecord(record); err != nil {
		return err
	}
	if _, err := loadRequestUnbounded(tx.unit, record.Key); err == nil {
		return conflict(llm.StoreConstraintRequestKey, record.Key)
	} else if !errors.Is(err, llm.ErrStoreRecordNotFound) {
		return err
	}
	if !record.ResponseComplete {
		if _, err := tx.FindActiveRequest(record.Task); err == nil {
			return conflict(llm.StoreConstraintActiveRequest, record.Task)
		} else if !errors.Is(err, llm.ErrStoreRecordNotFound) {
			return err
		}
	}
	if task, err := loadTask(tx.unit, record.Task); err != nil {
		return err
	} else if !task.Codec.Equal(record.Codec) {
		return conflict(llm.StoreConstraintImmutableRecord, record.Key)
	}
	return insertRequestRecord(tx.unit, record)
}

func insertRequestRecord(unit *unit, record llm.StoreRequestRecord) error {
	codec, err := encodeRecord(llm.StoreRecordRequest, record.Codec)
	if err != nil {
		return err
	}
	encoded, err := encodeRecord(llm.StoreRecordRequest, record)
	if err != nil {
		return err
	}
	_, err = unit.tx.ExecContext(unit.ctx, `
		INSERT INTO llm_requests (
		  caller_id, idempotency_key, task_caller_id, task_id,
		  request_id, response_id, request_digest, codec, mode,
		  canonical_payload, decision_status, decision_content_type,
		  decision_retry_after, decision_body, decision_body_is_nil, response_complete,
		  recovery_quarantined, last_event_sequence, revision,
		  created_at, completed_at, payload_pruned_at, record
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Key.Caller, record.Key.IdempotencyKey, record.Task.Caller, record.Task.Task,
		record.RequestID, record.ResponseID, record.RequestDigest, codec, record.Mode,
		nonNilBytes(record.CanonicalPayload), record.Decision.StatusCode, record.Decision.ContentType,
		record.Decision.RetryAfter, nonNilBytes(record.Decision.Body), boolInt(record.Decision.Body == nil), boolInt(record.ResponseComplete),
		boolInt(record.RecoveryQuarantined), record.LastEventSequence, record.Revision,
		unixNano(record.CreatedAt), nullableUnixNano(record.CompletedAt),
		nullableUnixNano(record.PayloadPrunedAt), encoded,
	)
	if err != nil {
		return fmt.Errorf("insert HumanLLM request: %w", err)
	}
	return nil
}

func loadRequestUnbounded(unit *unit, key llm.StoreRequestKey) (llm.StoreRequestRecord, error) {
	var encoded []byte
	err := unit.tx.QueryRowContext(unit.ctx, `
		SELECT record FROM llm_requests WHERE caller_id = ? AND idempotency_key = ?`,
		key.Caller, key.IdempotencyKey,
	).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreRequestRecord{}, notFound(llm.StoreRecordRequest, key)
	}
	if err != nil {
		return llm.StoreRequestRecord{}, fmt.Errorf("load HumanLLM request: %w", err)
	}
	var record llm.StoreRequestRecord
	if err := decodeRecord(llm.StoreRecordRequest, fmt.Sprint(key), encoded, &record); err != nil {
		return llm.StoreRequestRecord{}, err
	}
	return record, nil
}

func (tx *tx) CompareAndSwapRequest(mutation llm.StoreRequestMutation) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if !validRequestKey(mutation.Key) || mutation.ExpectedRevision == 0 || mutation.Next.Revision == 0 {
		return false, invalidArgument("invalid request mutation")
	}
	current, err := loadRequestUnbounded(tx.unit, mutation.Key)
	if err != nil {
		return false, err
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if !requestIdentityEqual(current, mutation.Next) || mutation.Next.Revision != current.Revision+1 {
		return false, conflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	if current.PayloadPrunedAt == nil {
		if mutation.Next.PayloadPrunedAt == nil {
			if !bytes.Equal(current.CanonicalPayload, mutation.Next.CanonicalPayload) {
				return false, conflict(llm.StoreConstraintImmutableRecord, mutation.Key)
			}
		} else if len(mutation.Next.CanonicalPayload) != 0 || len(mutation.Next.Decision.Body) != 0 {
			return false, conflict(llm.StoreConstraintImmutableRecord, mutation.Key)
		}
	} else if mutation.Next.PayloadPrunedAt == nil ||
		!mutation.Next.PayloadPrunedAt.Equal(*current.PayloadPrunedAt) ||
		len(mutation.Next.CanonicalPayload) != 0 || len(mutation.Next.Decision.Body) != 0 {
		return false, conflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	if !mutation.Next.ResponseComplete {
		found, err := tx.FindActiveRequest(mutation.Next.Task)
		if err == nil && found.Key != mutation.Key {
			return false, conflict(llm.StoreConstraintActiveRequest, mutation.Next.Task)
		}
		if err != nil && !errors.Is(err, llm.ErrStoreRecordNotFound) {
			return false, err
		}
	}
	codec, err := encodeRecord(llm.StoreRecordRequest, mutation.Next.Codec)
	if err != nil {
		return false, err
	}
	encoded, err := encodeRecord(llm.StoreRecordRequest, mutation.Next)
	if err != nil {
		return false, err
	}
	result, err := tx.unit.tx.ExecContext(tx.unit.ctx, `
		UPDATE llm_requests SET
		  codec = ?, canonical_payload = ?, decision_status = ?,
		  decision_content_type = ?, decision_retry_after = ?, decision_body = ?, decision_body_is_nil = ?,
		  response_complete = ?, recovery_quarantined = ?, last_event_sequence = ?,
		  revision = ?, completed_at = ?, payload_pruned_at = ?, record = ?
		WHERE caller_id = ? AND idempotency_key = ? AND revision = ?`,
		codec, nonNilBytes(mutation.Next.CanonicalPayload), mutation.Next.Decision.StatusCode,
		mutation.Next.Decision.ContentType, mutation.Next.Decision.RetryAfter,
		nonNilBytes(mutation.Next.Decision.Body), boolInt(mutation.Next.Decision.Body == nil), boolInt(mutation.Next.ResponseComplete),
		boolInt(mutation.Next.RecoveryQuarantined), mutation.Next.LastEventSequence,
		mutation.Next.Revision, nullableUnixNano(mutation.Next.CompletedAt),
		nullableUnixNano(mutation.Next.PayloadPrunedAt), encoded,
		mutation.Key.Caller, mutation.Key.IdempotencyKey, mutation.ExpectedRevision,
	)
	if err != nil {
		return false, fmt.Errorf("compare-and-swap HumanLLM request: %w", err)
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nonNilBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}

func (view *view) LoadWorkerReceipt(
	request llm.StoreRequestKey,
	eventID string,
) (llm.StoreWorkerReceiptRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreWorkerReceiptRecord{}, err
	}
	if !validRequestKey(request) || eventID == "" {
		return llm.StoreWorkerReceiptRecord{}, invalidArgument("invalid worker receipt key")
	}
	record := llm.StoreWorkerReceiptRecord{Request: request, EventID: eventID}
	var created int64
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT worker_id, digest, created_at FROM llm_worker_receipts
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ?`,
		request.Caller, request.IdempotencyKey, eventID,
	).Scan(&record.Worker, &record.Digest, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreWorkerReceiptRecord{}, notFound(llm.StoreRecordWorkerReceipt, eventID)
	}
	if err != nil {
		return llm.StoreWorkerReceiptRecord{}, fmt.Errorf("load HumanLLM worker receipt: %w", err)
	}
	record.CreatedAt = time.Unix(0, created).UTC()
	return record, nil
}

func (tx *tx) InsertWorkerReceipt(record llm.StoreWorkerReceiptRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := validateWorkerReceiptRecord(record); err != nil {
		return err
	}
	if _, err := tx.LoadWorkerReceipt(record.Request, record.EventID); err == nil {
		return conflict(llm.StoreConstraintWorkerReceipt, record.EventID)
	} else if !errors.Is(err, llm.ErrStoreRecordNotFound) {
		return err
	}
	_, err := tx.unit.tx.ExecContext(tx.unit.ctx, `
		INSERT INTO llm_worker_receipts (
		  caller_id, idempotency_key, event_id, worker_id, digest, created_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		record.Request.Caller, record.Request.IdempotencyKey, record.EventID,
		record.Worker, record.Digest, unixNano(record.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("insert HumanLLM worker receipt: %w", err)
	}
	return nil
}

func (tx *tx) InsertResponseEvent(record llm.StoreResponseEventRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := validateResponseEventRecord(record); err != nil {
		return err
	}
	var existingWorker, existingDigest string
	if record.WorkerEventID != "" {
		err := tx.unit.tx.QueryRowContext(tx.unit.ctx, `
				SELECT worker_id, worker_event_digest FROM llm_response_events
				WHERE caller_id = ? AND idempotency_key = ? AND worker_event_id = ?
				LIMIT 1`, record.Request.Caller, record.Request.IdempotencyKey, record.WorkerEventID,
		).Scan(&existingWorker, &existingDigest)
		if err == nil && (existingWorker != string(record.Worker) || existingDigest != string(record.WorkerEventDigest)) {
			return conflict(llm.StoreConstraintWorkerEvent, record.WorkerEventID)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("inspect HumanLLM worker event identity: %w", err)
		}
	}
	var exists int
	err := tx.unit.tx.QueryRowContext(tx.unit.ctx, `
		SELECT 1 FROM llm_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND sequence = ?`,
		record.Request.Caller, record.Request.IdempotencyKey, record.Sequence,
	).Scan(&exists)
	if err == nil {
		return conflict(llm.StoreConstraintResponseSequence, record.Sequence)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("inspect HumanLLM response event sequence: %w", err)
	}
	_, err = tx.unit.tx.ExecContext(tx.unit.ctx, `
		INSERT INTO llm_response_events (
		  caller_id, idempotency_key, sequence, kind, worker_id,
		  worker_event_id, worker_event_digest, data, data_is_nil, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Request.Caller, record.Request.IdempotencyKey, record.Sequence,
		record.Kind, record.Worker, record.WorkerEventID, record.WorkerEventDigest,
		nonNilBytes(record.Data), boolInt(record.Data == nil), unixNano(record.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("insert HumanLLM response event: %w", err)
	}
	return nil
}

func (view *view) LoadToolExecution(
	key llm.StoreToolExecutionKey,
	limit llm.StoreReadLimit,
) (llm.StoreToolExecutionRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	if !validToolKey(key) {
		return llm.StoreToolExecutionRecord{}, invalidArgument("invalid tool execution key")
	}
	if err := validateReadLimit(llm.StoreRecordToolExecution, limit); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	var encoded []byte
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT CASE WHEN length(result) <= ? THEN record ELSE NULL END,
		       length(result)
		FROM llm_tool_executions
		WHERE task_caller_id = ? AND task_id = ? AND tool_call_id = ?`,
		limit.MaxBytes, key.Task.Caller, key.Task.Task, key.ToolCallID,
	).Scan(&encoded, new(int64))
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreToolExecutionRecord{}, notFound(llm.StoreRecordToolExecution, key)
	}
	if err != nil {
		return llm.StoreToolExecutionRecord{}, fmt.Errorf("load HumanLLM tool execution: %w", err)
	}
	if encoded == nil {
		return llm.StoreToolExecutionRecord{}, &llm.StoreLimitError{Record: llm.StoreRecordToolExecution, Limit: limit.MaxBytes}
	}
	var record llm.StoreToolExecutionRecord
	if err := decodeRecord(llm.StoreRecordToolExecution, fmt.Sprint(key), encoded, &record); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	if int64(len(record.Result)) > limit.MaxBytes {
		return llm.StoreToolExecutionRecord{}, &llm.StoreLimitError{Record: llm.StoreRecordToolExecution, Limit: limit.MaxBytes}
	}
	if record.Key != key {
		return llm.StoreToolExecutionRecord{}, &llm.StoreCorruptError{
			Record: llm.StoreRecordToolExecution, Key: fmt.Sprint(key), Cause: errors.New("tool key does not match physical identity"),
		}
	}
	return record, nil
}

func loadToolUnbounded(unit *unit, key llm.StoreToolExecutionKey) (llm.StoreToolExecutionRecord, error) {
	var encoded []byte
	err := unit.tx.QueryRowContext(unit.ctx, `
		SELECT record FROM llm_tool_executions
		WHERE task_caller_id = ? AND task_id = ? AND tool_call_id = ?`,
		key.Task.Caller, key.Task.Task, key.ToolCallID,
	).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return llm.StoreToolExecutionRecord{}, notFound(llm.StoreRecordToolExecution, key)
	}
	if err != nil {
		return llm.StoreToolExecutionRecord{}, fmt.Errorf("load HumanLLM tool execution: %w", err)
	}
	var record llm.StoreToolExecutionRecord
	if err := decodeRecord(llm.StoreRecordToolExecution, fmt.Sprint(key), encoded, &record); err != nil {
		return llm.StoreToolExecutionRecord{}, err
	}
	return record, nil
}

func (tx *tx) InsertToolExecution(record llm.StoreToolExecutionRecord) error {
	if err := tx.unit.ensureActive(); err != nil {
		return err
	}
	if err := validateToolRecord(record); err != nil {
		return err
	}
	if _, err := loadToolUnbounded(tx.unit, record.Key); err == nil {
		return conflict(llm.StoreConstraintToolCall, record.Key)
	} else if !errors.Is(err, llm.ErrStoreRecordNotFound) {
		return err
	}
	encoded, err := encodeRecord(llm.StoreRecordToolExecution, record)
	if err != nil {
		return err
	}
	_, err = tx.unit.tx.ExecContext(tx.unit.ctx, `
		INSERT INTO llm_tool_executions (
		  task_caller_id, task_id, tool_call_id, input_digest, state, result,
		  result_is_nil, revision, created_at, record
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Key.Task.Caller, record.Key.Task.Task, record.Key.ToolCallID,
		record.InputDigest, record.State, nonNilBytes(record.Result), boolInt(record.Result == nil),
		record.Revision, unixNano(record.CreatedAt), encoded,
	)
	if err != nil {
		return fmt.Errorf("insert HumanLLM tool execution: %w", err)
	}
	return nil
}

func (tx *tx) CompareAndSwapToolExecution(
	mutation llm.StoreToolExecutionMutation,
) (bool, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return false, err
	}
	if !validToolKey(mutation.Key) || mutation.ExpectedRevision == 0 || mutation.Next.Revision == 0 {
		return false, invalidArgument("invalid tool execution mutation")
	}
	current, err := loadToolUnbounded(tx.unit, mutation.Key)
	if err != nil {
		return false, err
	}
	if current.Revision != mutation.ExpectedRevision {
		return false, nil
	}
	if !toolIdentityEqual(current, mutation.Next) || mutation.Next.Revision != current.Revision+1 {
		return false, conflict(llm.StoreConstraintImmutableRecord, mutation.Key)
	}
	encoded, err := encodeRecord(llm.StoreRecordToolExecution, mutation.Next)
	if err != nil {
		return false, err
	}
	result, err := tx.unit.tx.ExecContext(tx.unit.ctx, `
		UPDATE llm_tool_executions SET state = ?, result = ?, result_is_nil = ?, revision = ?, record = ?
		WHERE task_caller_id = ? AND task_id = ? AND tool_call_id = ? AND revision = ?`,
		mutation.Next.State, nonNilBytes(mutation.Next.Result), boolInt(mutation.Next.Result == nil),
		mutation.Next.Revision, encoded,
		mutation.Key.Task.Caller, mutation.Key.Task.Task, mutation.Key.ToolCallID,
		mutation.ExpectedRevision,
	)
	if err != nil {
		return false, fmt.Errorf("compare-and-swap HumanLLM tool execution: %w", err)
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}
