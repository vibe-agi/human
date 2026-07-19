package agent

import (
	"context"
	"errors"
	"fmt"
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
	store, release, err := agent.acquireStore()
	if err != nil {
		return Message{}, err
	}
	defer release()
	var message Message
	err = store.View(ctx, func(view StoreView) error {
		record, err := view.LoadMessage(
			StoreMessageKey{Authority: authority, ID: id},
			StoreReadLimit{MaxBytes: maxPageBytes},
		)
		if errors.Is(err, ErrStoreRecordNotFound) {
			return ErrNotFound
		}
		if errors.Is(err, ErrStoreRecordTooLarge) {
			return fmt.Errorf("%w: Agent message exceeds storage read budget", ErrCorruptStore)
		}
		if err != nil {
			return fmt.Errorf("get Agent message: %w", err)
		}
		if record.ID != id || record.Task.Workspace.Authority != authority {
			return fmt.Errorf("%w: Store returned a different Agent message identity", ErrCorruptStore)
		}
		if len(record.EncodedParts) == 0 || len(record.EncodedParts) > maxPageBytes {
			return fmt.Errorf("%w: Agent message exceeds storage read budget", ErrCorruptStore)
		}
		message, err = messageFromStoreRecord(record)
		return err
	})
	if err != nil {
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
	store, release, err := agent.acquireStore()
	if err != nil {
		return TaskRef{}, err
	}
	defer release()
	var ref TaskRef
	err = store.View(ctx, func(view StoreView) error {
		ref, err = view.ResolveTask(authority, id)
		if errors.Is(err, ErrStoreRecordNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("resolve Agent task: %w", err)
		}
		if ref.Workspace.Authority != authority || ref.ID != id {
			return fmt.Errorf("%w: Store returned a different resolved Task identity", ErrCorruptStore)
		}
		if err := validateTaskRef(ref); err != nil {
			return fmt.Errorf("%w: resolved invalid task key: %v", ErrCorruptStore, err)
		}
		return nil
	})
	if err != nil {
		return TaskRef{}, err
	}
	return ref, nil
}

// SnapshotTask returns a Task and the exact event cursor from one Store read
// snapshot. A subscriber can emit this snapshot, then call ReadEvents with the
// returned cursor without a snapshot-to-stream gap.
func (agent *Agent) SnapshotTask(ctx context.Context, ref TaskRef) (TaskSnapshot, error) {
	if err := validateCallContext(ctx); err != nil {
		return TaskSnapshot{}, err
	}
	if err := validateTaskRef(ref); err != nil {
		return TaskSnapshot{}, err
	}
	store, release, err := agent.acquireStore()
	if err != nil {
		return TaskSnapshot{}, err
	}
	defer release()
	var snapshot TaskSnapshot
	err = store.View(ctx, func(view StoreView) error {
		task, err := loadTaskFromStore(view, ref)
		if err != nil {
			return err
		}
		snapshot = TaskSnapshot{Task: task, EventCursor: task.EventCount}
		return nil
	})
	if err != nil {
		return TaskSnapshot{}, err
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

	store, release, err := agent.acquireStore()
	if err != nil {
		return TaskQueryPage{}, err
	}
	defer release()
	var page TaskQueryPage
	err = store.View(ctx, func(view StoreView) error {
		result, err := view.ScanAuthorityTasks(StoreTaskAuthorityScan{
			Authority:        authority,
			Context:          request.Context,
			State:            request.State,
			UpdatedAtOrAfter: request.UpdatedAtOrAfter,
			After:            request.After,
			Limit:            limit + 1,
		})
		if err != nil {
			return fmt.Errorf("list authority Agent tasks: %w", err)
		}
		if len(result.Records) > limit+1 || result.TotalSize < uint64(len(result.Records)) {
			return fmt.Errorf("%w: invalid Agent task query cardinality", ErrCorruptStore)
		}
		page = TaskQueryPage{
			Items:     make([]Task, 0, min(len(result.Records), limit)),
			TotalSize: result.TotalSize,
		}
		var previous *Task
		for index, record := range result.Records {
			if err := validateStoreTaskRecord(record); err != nil {
				return err
			}
			task := record.Task
			if err := validateAuthorityTaskQueryRecord(task, authority, request, previous); err != nil {
				return err
			}
			previous = &task
			if index == limit {
				page.HasMore = true
				break
			}
			page.Items = append(page.Items, cloneTask(task))
		}
		if page.HasMore && len(page.Items) > 0 {
			last := page.Items[len(page.Items)-1]
			page.Next = &TaskQueryCursor{
				UpdatedAt: last.UpdatedAt,
				Workspace: last.Ref.Workspace.ID,
				Task:      last.Ref.ID,
			}
		}
		return nil
	})
	if err != nil {
		return TaskQueryPage{}, err
	}
	return page, nil
}

func validateAuthorityTaskQueryRecord(
	task Task,
	authority AuthorityID,
	request TaskQuery,
	previous *Task,
) error {
	if task.Ref.Workspace.Authority != authority ||
		(request.Context != "" && task.Context.ID != request.Context) ||
		(request.State != "" && task.State != request.State) ||
		(request.UpdatedAtOrAfter != nil && task.UpdatedAt.Before(request.UpdatedAtOrAfter.UTC())) ||
		(request.After != nil && !taskFollowsAuthorityCursor(task, *request.After)) {
		return fmt.Errorf("%w: Agent Store returned a Task outside the query", ErrCorruptStore)
	}
	if previous != nil && !authorityTaskPrecedes(*previous, task) {
		return fmt.Errorf("%w: Agent Store returned Tasks out of order", ErrCorruptStore)
	}
	return nil
}

func taskFollowsAuthorityCursor(task Task, cursor TaskQueryCursor) bool {
	if task.UpdatedAt.Before(cursor.UpdatedAt) {
		return true
	}
	if !task.UpdatedAt.Equal(cursor.UpdatedAt) {
		return false
	}
	if task.Ref.Workspace.ID != cursor.Workspace {
		return task.Ref.Workspace.ID > cursor.Workspace
	}
	return task.Ref.ID > cursor.Task
}

func authorityTaskPrecedes(first, second Task) bool {
	if !first.UpdatedAt.Equal(second.UpdatedAt) {
		return first.UpdatedAt.After(second.UpdatedAt)
	}
	if first.Ref.Workspace.ID != second.Ref.Workspace.ID {
		return first.Ref.Workspace.ID < second.Ref.Workspace.ID
	}
	return first.Ref.ID < second.Ref.ID
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
