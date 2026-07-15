package delegation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const execSelect = `
	SELECT task_id, request_id, worker_id, command, cwd, timeout_ms, reason,
	       status, exit_code, stdout, stderr, error, truncated, timed_out,
	       request_digest, resolution_id, resolution_digest,
	       request_sequence, resolution_sequence, created_at, resolved_at
	FROM delegation_exec_requests`

// RequestExec records a command proposed by the authenticated task worker.
// humand never executes the command; it only persists and routes the request.
func (store *Store) RequestExec(ctx context.Context, input RequestExecInput) (ExecResult, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecResult{}, fmt.Errorf("begin delegation exec request: %w", err)
	}
	defer tx.Rollback()
	result, err := store.requestExecTx(ctx, tx, input)
	if err != nil {
		return ExecResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ExecResult{}, fmt.Errorf("commit delegation exec request: %w", err)
	}
	return result, nil
}

func (store *Store) requestExecTx(ctx context.Context, tx *sql.Tx, input RequestExecInput) (ExecResult, error) {
	if err := validateCommand(input.CommandInput); err != nil {
		return ExecResult{}, err
	}
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	input.RequestID = strings.TrimSpace(input.RequestID)
	input.CWD = strings.TrimSpace(input.CWD)
	input.Reason = strings.TrimSpace(input.Reason)
	if input.WorkerID == "" || input.RequestID == "" || strings.TrimSpace(input.Command) == "" || input.Reason == "" {
		return ExecResult{}, fmt.Errorf("%w: worker, request id, command, and reason are required", ErrInvalidInput)
	}
	if len(input.RequestID) > 128 || len(input.Command) > 64<<10 || len(input.CWD) > 4<<10 || len(input.Reason) > 8<<10 ||
		input.TimeoutMS < 0 || input.TimeoutMS > 60*60*1000 || strings.IndexByte(input.Command, 0) >= 0 {
		return ExecResult{}, fmt.Errorf("%w: command request exceeds protocol limits", ErrInvalidInput)
	}
	digest := execRequestDigest(input)
	task, err := getTask(ctx, tx, input.TaskID)
	if err != nil {
		return ExecResult{}, err
	}
	existing, storedDigest, _, err := getExecRequest(ctx, tx, input.TaskID, input.RequestID)
	if err == nil {
		if storedDigest != digest {
			return ExecResult{}, fmt.Errorf("%w: exec request %q", ErrIdempotencyConflict, input.RequestID)
		}
		event, err := getEvent(ctx, tx, task.ID, existing.RequestSequence)
		if err != nil {
			return ExecResult{}, err
		}
		return ExecResult{Task: task, Request: existing, Event: event, Replay: true}, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return ExecResult{}, err
	}
	if task.Revision != input.ExpectedRevision {
		return ExecResult{}, fmt.Errorf("%w: task %q is at revision %d, expected %d",
			ErrRevisionConflict, task.ID, task.Revision, input.ExpectedRevision)
	}
	if task.State != StateWorking {
		return ExecResult{}, transitionError(task.State, task.State)
	}
	if task.WorkerID != input.WorkerID {
		return ExecResult{}, fmt.Errorf("%w: task is leased to a different worker", ErrInvalidTransition)
	}
	now := store.now().UTC()
	task.Revision++
	task.UpdatedAt = now
	request := ExecRequest{
		TaskID: task.ID, ID: input.RequestID, WorkerID: input.WorkerID,
		Command: input.Command, CWD: input.CWD, TimeoutMS: input.TimeoutMS,
		Reason: input.Reason, Status: ExecPending, RequestSequence: task.Revision, CreatedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO delegation_exec_requests (
		  task_id, request_id, worker_id, command, cwd, timeout_ms, reason, status,
		  exit_code, stdout, stderr, error, truncated, timed_out, request_digest,
		  resolution_id, resolution_digest, request_sequence, resolution_sequence,
		  created_at, resolved_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', NULL, x'', x'', '', 0, 0, ?, '', '', ?, NULL, ?, NULL)`,
		request.TaskID, request.ID, request.WorkerID, request.Command, request.CWD,
		request.TimeoutMS, request.Reason, digest, request.RequestSequence, now.UnixNano()); err != nil {
		if isUniqueConstraint(err) {
			return ExecResult{}, fmt.Errorf("%w: exec request %q", ErrIdempotencyConflict, request.ID)
		}
		return ExecResult{}, fmt.Errorf("insert delegation exec request: %w", err)
	}
	if err := persistTask(ctx, tx, task, input.ExpectedRevision); err != nil {
		return ExecResult{}, err
	}
	eventData, _ := json.Marshal(request)
	event := Event{
		TaskID: task.ID, Sequence: task.Revision, Kind: EventExecRequested,
		FromState: task.State, ToState: task.State, Data: eventData, CreatedAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return ExecResult{}, err
	}
	return ExecResult{Task: task, Request: request, Event: event}, nil
}

// ResolveExec records a caller-side denial or the bounded execution result.
// ResolutionID is the caller's durable idempotency key.
func (store *Store) ResolveExec(ctx context.Context, input ResolveExecInput) (ExecResult, error) {
	if err := validateCommand(input.CommandInput); err != nil {
		return ExecResult{}, err
	}
	input.RequestID = strings.TrimSpace(input.RequestID)
	input.ResolutionID = strings.TrimSpace(input.ResolutionID)
	input.Error = strings.TrimSpace(input.Error)
	if input.RequestID == "" || input.ResolutionID == "" || len(input.RequestID) > 128 || len(input.ResolutionID) > 128 {
		return ExecResult{}, fmt.Errorf("%w: exec request and resolution ids are required", ErrInvalidInput)
	}
	if len(input.Stdout) > 8<<20 || len(input.Stderr) > 8<<20 || len(input.Error) > 64<<10 {
		return ExecResult{}, fmt.Errorf("%w: exec result exceeds protocol limits", ErrInvalidInput)
	}
	if !input.Approved && (len(input.Stdout) != 0 || len(input.Stderr) != 0 || input.TimedOut || input.Truncated) {
		return ExecResult{}, fmt.Errorf("%w: denied command cannot carry execution output", ErrInvalidInput)
	}
	digest := execResolutionDigest(input)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecResult{}, fmt.Errorf("begin delegation exec resolution: %w", err)
	}
	defer tx.Rollback()
	task, err := getTask(ctx, tx, input.TaskID)
	if err != nil {
		return ExecResult{}, err
	}
	request, _, storedResolutionDigest, err := getExecRequest(ctx, tx, task.ID, input.RequestID)
	if err != nil {
		return ExecResult{}, err
	}
	if request.Status != ExecPending {
		if request.ResolutionID != input.ResolutionID || storedResolutionDigest != digest || request.ResolutionSequence == nil {
			return ExecResult{}, fmt.Errorf("%w: exec resolution %q", ErrIdempotencyConflict, input.ResolutionID)
		}
		event, err := getEvent(ctx, tx, task.ID, *request.ResolutionSequence)
		if err != nil {
			return ExecResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return ExecResult{}, fmt.Errorf("commit delegation exec resolution replay: %w", err)
		}
		return ExecResult{Task: task, Request: request, Event: event, Replay: true}, nil
	}
	if task.Revision != input.ExpectedRevision {
		return ExecResult{}, fmt.Errorf("%w: task %q is at revision %d, expected %d",
			ErrRevisionConflict, task.ID, task.Revision, input.ExpectedRevision)
	}
	if task.State.IsTerminal() || task.State == StateRewindPending {
		return ExecResult{}, transitionError(task.State, task.State)
	}
	now := store.now().UTC()
	task.Revision++
	task.UpdatedAt = now
	request.ResolutionID = input.ResolutionID
	request.ResolutionSequence = int64Pointer(task.Revision)
	request.ResolvedAt = &now
	request.Error = input.Error
	request.Truncated = input.Truncated
	request.TimedOut = input.TimedOut
	request.Stdout = cloneBytes(input.Stdout)
	request.Stderr = cloneBytes(input.Stderr)
	kind := EventExecDenied
	if !input.Approved {
		request.Status = ExecDenied
		request.Stdout, request.Stderr = []byte{}, []byte{}
		if request.Error == "" {
			request.Error = "denied by caller"
		}
	} else {
		request.ExitCode = intPointer(input.ExitCode)
		kind = EventExecCompleted
		request.Status = ExecCompleted
		if input.Error != "" || input.TimedOut {
			kind = EventExecFailed
			request.Status = ExecFailed
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE delegation_exec_requests
		SET status = ?, exit_code = ?, stdout = ?, stderr = ?, error = ?, truncated = ?, timed_out = ?,
		    resolution_id = ?, resolution_digest = ?, resolution_sequence = ?, resolved_at = ?
		WHERE task_id = ? AND request_id = ? AND status = 'pending'`,
		request.Status, nullableInt(request.ExitCode), nonNilBytes(request.Stdout), nonNilBytes(request.Stderr),
		request.Error, request.Truncated, request.TimedOut, request.ResolutionID, digest,
		*request.ResolutionSequence, now.UnixNano(), request.TaskID, request.ID)
	if err != nil {
		if isUniqueConstraint(err) {
			return ExecResult{}, fmt.Errorf("%w: exec resolution %q", ErrIdempotencyConflict, input.ResolutionID)
		}
		return ExecResult{}, fmt.Errorf("update delegation exec request: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		if err != nil {
			return ExecResult{}, err
		}
		return ExecResult{}, fmt.Errorf("%w: exec request changed while resolving", ErrRevisionConflict)
	}
	if err := persistTask(ctx, tx, task, input.ExpectedRevision); err != nil {
		return ExecResult{}, err
	}
	eventData, _ := json.Marshal(request)
	event := Event{
		TaskID: task.ID, Sequence: task.Revision, Kind: kind,
		FromState: task.State, ToState: task.State, Data: eventData, CreatedAt: now,
	}
	if err := insertEvent(ctx, tx, event); err != nil {
		return ExecResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ExecResult{}, fmt.Errorf("commit delegation exec resolution: %w", err)
	}
	return ExecResult{Task: task, Request: request, Event: event}, nil
}

func (store *Store) ListExecRequests(ctx context.Context, taskID string) ([]ExecRequest, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, fmt.Errorf("%w: task id is required", ErrInvalidInput)
	}
	rows, err := store.db.QueryContext(ctx, execSelect+` WHERE task_id = ? ORDER BY request_sequence`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list delegation exec requests: %w", err)
	}
	defer rows.Close()
	var requests []ExecRequest
	for rows.Next() {
		request, _, _, err := scanExecRequest(rows)
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delegation exec requests: %w", err)
	}
	return requests, nil
}

func getExecRequest(ctx context.Context, queryer queryRower, taskID, requestID string) (ExecRequest, string, string, error) {
	row := queryer.QueryRowContext(ctx, execSelect+` WHERE task_id = ? AND request_id = ?`, taskID, requestID)
	return scanExecRequest(row)
}

func scanExecRequest(row rowScanner) (ExecRequest, string, string, error) {
	var request ExecRequest
	var status string
	var exitCode, resolutionSequence, resolvedAt sql.NullInt64
	var truncated, timedOut bool
	var requestDigest, resolutionDigest string
	var createdAt int64
	if err := row.Scan(
		&request.TaskID, &request.ID, &request.WorkerID, &request.Command, &request.CWD,
		&request.TimeoutMS, &request.Reason, &status, &exitCode, &request.Stdout, &request.Stderr,
		&request.Error, &truncated, &timedOut, &requestDigest, &request.ResolutionID,
		&resolutionDigest, &request.RequestSequence, &resolutionSequence, &createdAt, &resolvedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecRequest{}, "", "", ErrNotFound
		}
		return ExecRequest{}, "", "", fmt.Errorf("scan delegation exec request: %w", err)
	}
	request.Status = ExecStatus(status)
	request.ExitCode = nullableInt64AsInt(exitCode)
	request.Truncated = truncated
	request.TimedOut = timedOut
	request.ResolutionSequence = nullableInt64(resolutionSequence)
	request.CreatedAt = fromUnixNano(createdAt)
	if resolvedAt.Valid {
		value := fromUnixNano(resolvedAt.Int64)
		request.ResolvedAt = &value
	}
	return request, requestDigest, resolutionDigest, nil
}

func execRequestDigest(input RequestExecInput) string {
	payload, _ := json.Marshal(struct {
		WorkerID  string `json:"worker_id"`
		Command   string `json:"command"`
		CWD       string `json:"cwd"`
		TimeoutMS int64  `json:"timeout_ms"`
		Reason    string `json:"reason"`
	}{input.WorkerID, input.Command, input.CWD, input.TimeoutMS, input.Reason})
	return digestBytes(payload)
}

func execResolutionDigest(input ResolveExecInput) string {
	payload, _ := json.Marshal(struct {
		Approved  bool   `json:"approved"`
		ExitCode  int    `json:"exit_code"`
		Stdout    []byte `json:"stdout"`
		Stderr    []byte `json:"stderr"`
		Error     string `json:"error"`
		Truncated bool   `json:"truncated"`
		TimedOut  bool   `json:"timed_out"`
	}{input.Approved, input.ExitCode, input.Stdout, input.Stderr, strings.TrimSpace(input.Error), input.Truncated, input.TimedOut})
	return digestBytes(payload)
}

func digestBytes(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableInt64AsInt(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	converted := int(value.Int64)
	return &converted
}

func intPointer(value int) *int { return &value }
