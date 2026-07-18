package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// GetMessage resolves a globally unique message inside one authenticated
// Authority. It is primarily useful to recognize an exact transport retry
// after the Task revision has already advanced.
func (agent *Agent) GetMessage(ctx context.Context, authority AuthorityID, id MessageID) (Message, error) {
	if err := validateCallContext(ctx); err != nil {
		return Message{}, err
	}
	if err := validateStable("authority id", string(authority)); err != nil {
		return Message{}, err
	}
	if err := validateStable("message id", string(id)); err != nil {
		return Message{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return Message{}, err
	}
	defer release()
	var message Message
	var parts []byte
	var digest string
	var created int64
	message.ID = id
	message.Task.Workspace.Authority = authority
	err = database.QueryRowContext(ctx, `
		SELECT workspace_id, task_id, sequence, author,
		       CASE WHEN length(parts) <= ? THEN parts ELSE NULL END,
		       digest, created_at
		FROM agent_messages
		WHERE authority_id = ? AND message_id = ?`, maxPageBytes, authority, id,
	).Scan(
		&message.Task.Workspace.ID, &message.Task.ID, &message.Sequence,
		&message.Author, &parts, &digest, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, ErrNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("get Agent message: %w", err)
	}
	if parts == nil {
		return Message{}, fmt.Errorf("%w: Agent message exceeds storage read budget", ErrCorruptStore)
	}
	if err := json.Unmarshal(parts, &message.Parts); err != nil {
		return Message{}, fmt.Errorf("%w: decode Agent message %q: %v", ErrCorruptStore, id, err)
	}
	actual, err := contentDigest(message.Parts)
	if err != nil || actual != digest {
		return Message{}, fmt.Errorf("%w: Agent message %q content digest mismatch", ErrCorruptStore, id)
	}
	message.CreatedAt = fromUnixNano(created)
	if err := validateStoredMessage(message); err != nil {
		return Message{}, err
	}
	return cloneMessage(message), nil
}

// ResolveTask maps an authority-scoped public Task ID to the internal
// Workspace-qualified correctness key. Task IDs are unique only inside the
// authenticated Authority; callers must never use a tenant or wire-provided
// Workspace as a substitute for this lookup.
func (agent *Agent) ResolveTask(ctx context.Context, authority AuthorityID, id TaskID) (TaskRef, error) {
	if err := validateCallContext(ctx); err != nil {
		return TaskRef{}, err
	}
	if err := validateStable("authority id", string(authority)); err != nil {
		return TaskRef{}, err
	}
	if err := validateStable("task id", string(id)); err != nil {
		return TaskRef{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return TaskRef{}, err
	}
	defer release()
	var workspace WorkspaceID
	err = database.QueryRowContext(ctx, `
		SELECT workspace_id
		FROM agent_tasks
		WHERE authority_id = ? AND task_id = ?`, authority, id,
	).Scan(&workspace)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskRef{}, ErrNotFound
	}
	if err != nil {
		return TaskRef{}, fmt.Errorf("resolve Agent task: %w", err)
	}
	ref := TaskRef{Workspace: WorkspaceRef{Authority: authority, ID: workspace}, ID: id}
	if err := validateTaskRef(ref); err != nil {
		return TaskRef{}, fmt.Errorf("%w: resolved invalid task key: %v", ErrCorruptStore, err)
	}
	return ref, nil
}

// SnapshotTask returns a Task and the exact event cursor from one SQLite read
// snapshot. A subscriber can emit this snapshot, then call ReadEvents with the
// returned cursor without a snapshot-to-stream gap.
func (agent *Agent) SnapshotTask(ctx context.Context, ref TaskRef) (TaskSnapshot, error) {
	if err := validateCallContext(ctx); err != nil {
		return TaskSnapshot{}, err
	}
	if err := validateTaskRef(ref); err != nil {
		return TaskSnapshot{}, err
	}
	database, release, err := agent.acquire()
	if err != nil {
		return TaskSnapshot{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return TaskSnapshot{}, fmt.Errorf("begin Agent task snapshot: %w", err)
	}
	defer tx.Rollback()
	task, err := loadTask(ctx, tx, ref)
	if err != nil {
		return TaskSnapshot{}, err
	}
	snapshot := TaskSnapshot{Task: task, EventCursor: task.EventCount}
	if err := tx.Commit(); err != nil {
		return TaskSnapshot{}, fmt.Errorf("commit Agent task snapshot: %w", err)
	}
	return snapshot, nil
}

// ListAuthorityTasks returns a live authority-wide view ordered by UpdatedAt
// descending. It is intended for task transports such as A2A, whose public task
// identifier omits Workspace and whose list operation spans Contexts.
func (agent *Agent) ListAuthorityTasks(ctx context.Context, authority AuthorityID, request TaskQuery) (TaskQueryPage, error) {
	if err := validateCallContext(ctx); err != nil {
		return TaskQueryPage{}, err
	}
	if err := validateStable("authority id", string(authority)); err != nil {
		return TaskQueryPage{}, err
	}
	limit, err := normalizePageLimit(request.Limit)
	if err != nil {
		return TaskQueryPage{}, err
	}
	if request.Context != "" {
		if err := validateStable("context id", string(request.Context)); err != nil {
			return TaskQueryPage{}, err
		}
	}
	if request.State != "" && !validTaskState(request.State) {
		return TaskQueryPage{}, fmt.Errorf("%w: unsupported task state %q", ErrInvalidArgument, request.State)
	}
	if request.UpdatedAtOrAfter != nil {
		if err := validateQueryTime("updated_at_or_after", *request.UpdatedAtOrAfter); err != nil {
			return TaskQueryPage{}, err
		}
	}
	if request.After != nil {
		if err := validateTaskQueryCursor(*request.After); err != nil {
			return TaskQueryPage{}, err
		}
	}

	database, release, err := agent.acquire()
	if err != nil {
		return TaskQueryPage{}, err
	}
	defer release()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return TaskQueryPage{}, fmt.Errorf("begin Agent task query: %w", err)
	}
	defer tx.Rollback()

	filters := []string{"t.authority_id = ?"}
	filterArgs := []any{authority}
	if request.Context != "" {
		filters = append(filters, "t.context_id = ?")
		filterArgs = append(filterArgs, request.Context)
	}
	if request.State != "" {
		filters = append(filters, "t.state = ?")
		filterArgs = append(filterArgs, request.State)
	}
	if request.UpdatedAtOrAfter != nil {
		filters = append(filters, "t.updated_at >= ?")
		filterArgs = append(filterArgs, unixNano(request.UpdatedAtOrAfter.UTC()))
	}
	where := strings.Join(filters, " AND ")
	var total int64
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM agent_tasks AS t WHERE "+where,
		filterArgs...,
	).Scan(&total); err != nil {
		return TaskQueryPage{}, fmt.Errorf("count Agent tasks: %w", err)
	}
	if total < 0 {
		return TaskQueryPage{}, fmt.Errorf("%w: negative Agent task count", ErrCorruptStore)
	}

	pageWhere := where
	pageArgs := append([]any(nil), filterArgs...)
	if request.After != nil {
		updated := unixNano(request.After.UpdatedAt.UTC())
		pageWhere += ` AND (t.updated_at < ? OR
			(t.updated_at = ? AND
			 (t.workspace_id > ? OR
			  (t.workspace_id = ? AND t.task_id > ?))))`
		pageArgs = append(pageArgs, updated, updated, request.After.Workspace, request.After.Workspace, request.After.Task)
	}
	pageArgs = append(pageArgs, limit+1)
	rows, err := tx.QueryContext(ctx, `
		SELECT t.workspace_id, t.task_id, t.context_id, t.state, t.revision,
		       t.message_count, t.event_count, t.created_at, t.updated_at,
		       a.artifact_id, a.state,
		       s.submission_id, s.final_message_id, s.artifact_id, s.published_at
		FROM agent_tasks AS t
		LEFT JOIN agent_artifacts AS a
		  ON a.authority_id = t.authority_id
		 AND a.workspace_id = t.workspace_id
		 AND a.task_id = t.task_id
		LEFT JOIN agent_submissions AS s
		  ON s.authority_id = t.authority_id
		 AND s.workspace_id = t.workspace_id
		 AND s.task_id = t.task_id
		WHERE `+pageWhere+`
		ORDER BY t.updated_at DESC, t.workspace_id, t.task_id
		LIMIT ?`, pageArgs...)
	if err != nil {
		return TaskQueryPage{}, fmt.Errorf("list authority Agent tasks: %w", err)
	}
	defer rows.Close()
	items := make([]Task, 0, limit+1)
	for rows.Next() {
		task, err := scanTask(rows, authority, true)
		if err != nil {
			return TaskQueryPage{}, err
		}
		items = append(items, cloneTask(task))
	}
	if err := rows.Err(); err != nil {
		return TaskQueryPage{}, fmt.Errorf("list authority Agent tasks: %w", err)
	}
	if err := rows.Close(); err != nil {
		return TaskQueryPage{}, fmt.Errorf("close authority Agent task page: %w", err)
	}
	page := TaskQueryPage{Items: items, TotalSize: uint64(total)}
	if len(page.Items) > limit {
		page.HasMore = true
		page.Items = page.Items[:limit]
	}
	if page.HasMore && len(page.Items) > 0 {
		last := page.Items[len(page.Items)-1]
		page.Next = &TaskQueryCursor{
			UpdatedAt: last.UpdatedAt,
			Workspace: last.Ref.Workspace.ID,
			Task:      last.Ref.ID,
		}
	}
	if err := tx.Commit(); err != nil {
		return TaskQueryPage{}, fmt.Errorf("commit Agent task query: %w", err)
	}
	return page, nil
}

func validateTaskQueryCursor(cursor TaskQueryCursor) error {
	if err := validateQueryTime("task query cursor", cursor.UpdatedAt); err != nil {
		return err
	}
	if err := validateStable("cursor workspace id", string(cursor.Workspace)); err != nil {
		return err
	}
	return validateStable("cursor task id", string(cursor.Task))
}

func validateQueryTime(label string, value time.Time) error {
	if value.IsZero() || !fromUnixNano(unixNano(value.UTC())).Equal(value.UTC()) {
		return fmt.Errorf("%w: %s is outside SQLite nanosecond range", ErrInvalidArgument, label)
	}
	return nil
}
