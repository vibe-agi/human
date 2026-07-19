package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vibe-agi/human/llm"
)

func (view *view) ScanResponseEvents(
	scan llm.StoreResponseEventScan,
) ([]llm.StoreResponseEventRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if !validRequestKey(scan.Request) {
		return nil, invalidArgument("invalid response-event request key")
	}
	if err := validateScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	if err := validateReadLimit(llm.StoreRecordResponseEvent, scan.ReadLimit); err != nil {
		return nil, err
	}
	query := `
		SELECT sequence, kind, worker_event_id, worker_event_digest,
		       CASE WHEN length(data) <= ? THEN data ELSE NULL END,
		       length(data), data_is_nil, length(data) <= ?, created_at
		FROM llm_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND sequence > ?`
	arguments := []any{scan.ReadLimit.MaxBytes, scan.ReadLimit.MaxBytes, scan.Request.Caller, scan.Request.IdempotencyKey, scan.After}
	if len(scan.Kinds) != 0 {
		query += " AND kind IN (" + strings.TrimRight(strings.Repeat("?,", len(scan.Kinds)), ",") + ")"
		seen := make(map[llm.StoreResponseEventKind]struct{}, len(scan.Kinds))
		for _, kind := range scan.Kinds {
			if kind != llm.StoreEventCheckpoint && kind != llm.StoreEventWire {
				return nil, invalidArgument("invalid response-event kind %q", kind)
			}
			if _, duplicate := seen[kind]; duplicate {
				return nil, invalidArgument("duplicate response-event kind %q", kind)
			}
			seen[kind] = struct{}{}
			arguments = append(arguments, kind)
		}
	}
	if scan.WorkerEventID != "" {
		query += " AND worker_event_id = ?"
		arguments = append(arguments, scan.WorkerEventID)
	}
	query += " ORDER BY sequence LIMIT ?"
	arguments = append(arguments, scan.Limit)
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("scan HumanLLM response events: %w", err)
	}
	defer rows.Close()
	result := make([]llm.StoreResponseEventRecord, 0, scan.Limit)
	var used int64
	for rows.Next() {
		record := llm.StoreResponseEventRecord{Request: scan.Request}
		var data []byte
		var size, created int64
		var dataIsNil, fits int
		if err := rows.Scan(
			&record.Sequence, &record.Kind, &record.WorkerEventID,
			&record.WorkerEventDigest, &data, &size, &dataIsNil, &fits, &created,
		); err != nil {
			return nil, fmt.Errorf("materialize HumanLLM response event: %w", err)
		}
		if fits == 0 || size > scan.ReadLimit.MaxBytes {
			if len(result) == 0 {
				return nil, &llm.StoreLimitError{Record: llm.StoreRecordResponseEvent, Limit: scan.ReadLimit.MaxBytes}
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		if dataIsNil == 0 {
			record.Data = make([]byte, len(data))
			copy(record.Data, data)
		}
		record.CreatedAt = time.Unix(0, created).UTC()
		result = append(result, record)
		used += size
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HumanLLM response events: %w", err)
	}
	return result, nil
}

func (view *view) ScanToolExecutions(
	scan llm.StoreToolExecutionScan,
) ([]llm.StoreToolExecutionRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if !validTaskKey(scan.Task) {
		return nil, invalidArgument("invalid tool-execution task key")
	}
	if scan.State != "" && scan.State != llm.ToolExecutionPending && scan.State != llm.ToolExecutionCompleted {
		return nil, invalidArgument("invalid tool-execution state %q", scan.State)
	}
	if err := validateScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	if err := validateReadLimit(llm.StoreRecordToolExecution, scan.ReadLimit); err != nil {
		return nil, err
	}
	query := `
		SELECT CASE WHEN length(result) <= ? THEN record ELSE NULL END, length(result)
		FROM llm_tool_executions
		WHERE task_caller_id = ? AND task_id = ? AND tool_call_id > ?`
	arguments := []any{scan.ReadLimit.MaxBytes, scan.Task.Caller, scan.Task.Task, scan.After}
	if scan.State != "" {
		query += " AND state = ?"
		arguments = append(arguments, scan.State)
	}
	query += " ORDER BY tool_call_id LIMIT ?"
	arguments = append(arguments, scan.Limit)
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("scan HumanLLM tool executions: %w", err)
	}
	defer rows.Close()
	result := make([]llm.StoreToolExecutionRecord, 0, scan.Limit)
	var used int64
	for rows.Next() {
		var encoded []byte
		var size int64
		if err := rows.Scan(&encoded, &size); err != nil {
			return nil, fmt.Errorf("materialize HumanLLM tool execution: %w", err)
		}
		if size > scan.ReadLimit.MaxBytes || encoded == nil {
			if len(result) == 0 {
				return nil, &llm.StoreLimitError{Record: llm.StoreRecordToolExecution, Limit: scan.ReadLimit.MaxBytes}
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		var record llm.StoreToolExecutionRecord
		if err := decodeRecord(llm.StoreRecordToolExecution, string(scan.After), encoded, &record); err != nil {
			return nil, err
		}
		result = append(result, record)
		used += size
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HumanLLM tool executions: %w", err)
	}
	return result, nil
}

func (view *view) ScanRecovery(scan llm.StoreRecoveryScan) ([]llm.StoreRecoveryRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := validateScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	if err := validateReadLimit(llm.StoreRecordRequest, scan.ReadLimit); err != nil {
		return nil, err
	}
	query := `
		SELECT r.caller_id, r.idempotency_key, r.task_caller_id, r.task_id,
		       CASE WHEN length(r.canonical_payload) + length(r.decision_body) <= ?
		            THEN r.record ELSE NULL END,
		       length(r.canonical_payload) + length(r.decision_body), t.record
		FROM llm_requests AS r
		JOIN llm_tasks AS t
		  ON t.caller_id = r.task_caller_id AND t.task_id = r.task_id
		WHERE (
		  (r.recovery_quarantined = 0 AND r.response_complete = 0)
		  OR EXISTS (
		    SELECT 1 FROM llm_response_events AS e
		    WHERE e.caller_id = r.caller_id
		      AND e.idempotency_key = r.idempotency_key
		      AND e.worker_event_id <> ''
		      AND NOT EXISTS (
		        SELECT 1 FROM llm_worker_receipts AS w
		        WHERE w.caller_id = e.caller_id
		          AND w.idempotency_key = e.idempotency_key
		          AND w.event_id = e.worker_event_id
		          AND w.digest = e.worker_event_digest
		      )
		  )
		)`
	arguments := []any{scan.ReadLimit.MaxBytes}
	if scan.After != nil {
		query += ` AND (
		  r.created_at > ?
		  OR (r.created_at = ? AND r.caller_id > ?)
		  OR (r.created_at = ? AND r.caller_id = ? AND r.idempotency_key > ?)
		)`
		created := unixNano(scan.After.CreatedAt)
		arguments = append(arguments, created, created, scan.After.Caller,
			created, scan.After.Caller, scan.After.IdempotencyKey)
	}
	query += " ORDER BY r.created_at, r.caller_id, r.idempotency_key LIMIT ?"
	arguments = append(arguments, scan.Limit)
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("scan HumanLLM recovery records: %w", err)
	}
	defer rows.Close()
	result := make([]llm.StoreRecoveryRecord, 0, scan.Limit)
	var used int64
	for rows.Next() {
		var key llm.StoreRequestKey
		var taskKey llm.StoreTaskKey
		var requestBytes, taskBytes []byte
		var size int64
		if err := rows.Scan(
			&key.Caller, &key.IdempotencyKey, &taskKey.Caller, &taskKey.Task,
			&requestBytes, &size, &taskBytes,
		); err != nil {
			return nil, fmt.Errorf("materialize HumanLLM recovery record: %w", err)
		}
		if size > scan.ReadLimit.MaxBytes || requestBytes == nil {
			if len(result) == 0 {
				return nil, &llm.StoreLimitError{Record: llm.StoreRecordRequest, Limit: scan.ReadLimit.MaxBytes}
			}
			break
		}
		if used+size > scan.ReadLimit.MaxBytes {
			break
		}
		var record llm.StoreRecoveryRecord
		if err := decodeRecord(llm.StoreRecordRequest, fmt.Sprint(key), requestBytes, &record.Request); err != nil {
			return nil, err
		}
		if err := decodeRecord(llm.StoreRecordTask, fmt.Sprint(taskKey), taskBytes, &record.Task); err != nil {
			return nil, err
		}
		if record.Request.Key != key || record.Task.Key != taskKey || record.Request.Task != taskKey {
			return nil, &llm.StoreCorruptError{Record: llm.StoreRecordRecoveryCursor, Key: fmt.Sprint(key), Cause: errors.New("recovery identity mismatch")}
		}
		result = append(result, record)
		used += size
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HumanLLM recovery records: %w", err)
	}
	return result, nil
}

func (view *view) ScanRetention(scan llm.StoreRetentionScan) ([]llm.StoreRetentionCandidate, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if scan.CompletedBefore.IsZero() {
		return nil, invalidArgument("retention completion boundary is required")
	}
	if err := validateScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	query := `
		SELECT caller_id, idempotency_key, task_caller_id, task_id,
		       request_id, response_id, request_digest, codec, mode,
		       decision_status, decision_content_type, decision_retry_after,
		       response_complete, recovery_quarantined, last_event_sequence,
		       revision, created_at, completed_at, payload_pruned_at,
		       COALESCE(completed_at, created_at),
		       EXISTS (
		         SELECT 1 FROM llm_response_events AS e
		         WHERE e.caller_id = llm_requests.caller_id
		           AND e.idempotency_key = llm_requests.idempotency_key
		           AND e.worker_event_id <> ''
		           AND NOT EXISTS (
		             SELECT 1 FROM llm_worker_receipts AS w
		             WHERE w.caller_id = e.caller_id
		               AND w.idempotency_key = e.idempotency_key
		               AND w.event_id = e.worker_event_id
		               AND w.digest = e.worker_event_digest
		           )
		       )
		FROM llm_requests
		WHERE response_complete = 1 AND payload_pruned_at IS NULL
		  AND COALESCE(completed_at, created_at) <= ?`
	arguments := []any{unixNano(scan.CompletedBefore)}
	if scan.After != nil {
		query += ` AND (
		  COALESCE(completed_at, created_at) > ?
		  OR (COALESCE(completed_at, created_at) = ? AND caller_id > ?)
		  OR (COALESCE(completed_at, created_at) = ? AND caller_id = ? AND idempotency_key > ?)
		)`
		completed := unixNano(scan.After.CompletedAt)
		arguments = append(arguments, completed, completed, scan.After.Caller,
			completed, scan.After.Caller, scan.After.IdempotencyKey)
	}
	query += " ORDER BY COALESCE(completed_at, created_at), caller_id, idempotency_key LIMIT ?"
	arguments = append(arguments, scan.Limit)
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("scan HumanLLM retention candidates: %w", err)
	}
	defer rows.Close()
	result := make([]llm.StoreRetentionCandidate, 0, scan.Limit)
	for rows.Next() {
		candidate, err := scanRetentionCandidate(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HumanLLM retention candidates: %w", err)
	}
	return result, nil
}

func scanRetentionCandidate(row rowScanner) (llm.StoreRetentionCandidate, error) {
	var candidate llm.StoreRetentionCandidate
	var codecBytes []byte
	var responseComplete, quarantined, unacknowledged int
	var created, effective int64
	var completed, pruned sql.NullInt64
	err := row.Scan(
		&candidate.Request.Key.Caller, &candidate.Request.Key.IdempotencyKey,
		&candidate.Request.Task.Caller, &candidate.Request.Task.Task,
		&candidate.Request.RequestID, &candidate.Request.ResponseID,
		&candidate.Request.RequestDigest, &codecBytes, &candidate.Request.Mode,
		&candidate.Request.DecisionStatus, &candidate.Request.DecisionContentType,
		&candidate.Request.DecisionRetryAfter, &responseComplete, &quarantined,
		&candidate.Request.LastEventSequence, &candidate.Request.Revision,
		&created, &completed, &pruned, &effective, &unacknowledged,
	)
	if err != nil {
		return llm.StoreRetentionCandidate{}, fmt.Errorf("materialize HumanLLM retention candidate: %w", err)
	}
	if err := decodeRecord(llm.StoreRecordRequest, fmt.Sprint(candidate.Request.Key)+"/codec", codecBytes, &candidate.Request.Codec); err != nil {
		return llm.StoreRetentionCandidate{}, err
	}
	candidate.Request.ResponseComplete = responseComplete != 0
	candidate.Request.RecoveryQuarantined = quarantined != 0
	candidate.Request.CreatedAt = time.Unix(0, created).UTC()
	candidate.Request.CompletedAt = timeFromNullable(completed)
	candidate.Request.PayloadPrunedAt = timeFromNullable(pruned)
	candidate.EffectiveCompletedAt = time.Unix(0, effective).UTC()
	candidate.UnacknowledgedWorkerEvent = unacknowledged != 0
	return candidate, nil
}

func (tx *tx) DeleteTombstonedResponseEvents(key llm.StoreRequestKey) (uint64, error) {
	if err := tx.unit.ensureActive(); err != nil {
		return 0, err
	}
	if !validRequestKey(key) {
		return 0, invalidArgument("invalid request key")
	}
	request, err := loadRequestUnbounded(tx.unit, key)
	if err != nil {
		return 0, err
	}
	if request.PayloadPrunedAt == nil {
		return 0, conflict(llm.StoreConstraintCompareAndSwap, key)
	}
	result, err := tx.unit.tx.ExecContext(tx.unit.ctx, `
		DELETE FROM llm_response_events WHERE caller_id = ? AND idempotency_key = ?`,
		key.Caller, key.IdempotencyKey,
	)
	if err != nil {
		return 0, fmt.Errorf("delete tombstoned HumanLLM response events: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return uint64(count), nil
}
