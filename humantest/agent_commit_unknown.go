package humantest

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

// CommitUnknownAgentStore wraps an agent.Store and injects the most dangerous
// Store outcome a driver can produce: agent.ErrStoreCommitUnknown, the "this
// commit may or may not be durable" classification whose reconciliation is the
// Agent command receipt: an exact retry must observe either the whole receipt
// and replay it, or no receipt and safely attempt the command.
//
// Two one-shot failure modes model the two real ambiguities:
//
//   - ArmCommittedUnknown: the next Update commits through the wrapped Store
//     and then reports commit-unknown, as when a database applied the
//     transaction but the acknowledgement was lost.
//   - ArmLostUnknown: the next Update performs no write at all and reports
//     commit-unknown, as when the connection died before the commit reached
//     the database.
//
// The wrapper is safe for concurrent use and composes with any conforming
// Store, so third-party adapters can rehearse ambiguous commits against the
// real Agent domain via TestAgentCommitUnknownReconciliation.
type CommitUnknownAgentStore struct {
	inner     agent.Store
	committed atomic.Bool
	lost      atomic.Bool
}

// WrapCommitUnknownAgentStore borrows inner; the caller keeps ownership and
// releases it after the wrapper is no longer used.
func WrapCommitUnknownAgentStore(inner agent.Store) *CommitUnknownAgentStore {
	return &CommitUnknownAgentStore{inner: inner}
}

// ArmCommittedUnknown makes exactly one subsequent Update commit durably and
// then report agent.ErrStoreCommitUnknown.
func (store *CommitUnknownAgentStore) ArmCommittedUnknown() { store.committed.Store(true) }

// ArmLostUnknown makes exactly one subsequent Update skip the inner commit
// entirely and report agent.ErrStoreCommitUnknown.
func (store *CommitUnknownAgentStore) ArmLostUnknown() { store.lost.Store(true) }

func (store *CommitUnknownAgentStore) Description() agent.StoreDescription {
	return store.inner.Description()
}

func (store *CommitUnknownAgentStore) View(ctx context.Context, callback func(agent.StoreView) error) error {
	return store.inner.View(ctx, callback)
}

func (store *CommitUnknownAgentStore) Update(ctx context.Context, callback func(agent.StoreTx) error) error {
	if store.lost.Swap(false) {
		return &agent.StoreCommitUnknownError{Cause: errors.New("humantest: injected pre-commit connection loss")}
	}
	err := store.inner.Update(ctx, callback)
	if err == nil && store.committed.Swap(false) {
		return &agent.StoreCommitUnknownError{Cause: errors.New("humantest: injected lost commit acknowledgement")}
	}
	return err
}

var _ agent.Store = (*CommitUnknownAgentStore)(nil)

// TestAgentCommitUnknownReconciliation drives the real Agent domain over a
// factory-provided Store while injecting ambiguous commits, proving that the
// Store's command receipts support the reconciliation contract:
//
//   - a caller command whose commit acknowledgement was lost surfaces
//     commit-unknown, and the exact retry replays the durable receipt without
//     creating a second Task;
//   - a caller command whose commit was genuinely lost fails without durable
//     effects, and the exact retry then performs the command exactly once;
//   - a fenced worker mutation whose commit acknowledgement was lost replays
//     its receipt on exact retry without advancing the Task twice.
//
// Passing complements TestAgentStore; it does not replace adapter-specific
// crash, durability, and infrastructure fault-injection tests.
func TestAgentCommitUnknownReconciliation(t *testing.T, factory AgentStoreFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("Agent Store conformance factory is nil")
	}

	t.Run("committed_unknown_create_replays_exactly_once", func(t *testing.T) {
		service, wrapper := openCommitUnknownAgent(t, factory)
		contextRef, taskRef := commitUnknownRefs("task-committed")
		create := commitUnknownCreate(contextRef, taskRef, "cmd-create", "start")
		wrapper.ArmCommittedUnknown()
		if _, err := service.CreateTask(t.Context(), create); !errors.Is(err, agent.ErrStoreCommitUnknown) {
			t.Fatalf("commit-unknown create = %v, want ErrStoreCommitUnknown", err)
		}
		task, err := service.CreateTask(t.Context(), create)
		if err != nil {
			t.Fatalf("exact retry did not replay the committed receipt: %v", err)
		}
		if task.State != agent.TaskSubmitted || task.Revision != 1 || task.MessageCount != 1 {
			t.Fatalf("replayed task = %#v", task)
		}
		assertSingleCommitUnknownTask(t, service, contextRef)
		conflicting := create
		conflicting.Message = commitUnknownMessage("message-1", "different")
		if _, err := service.CreateTask(t.Context(), conflicting); !errors.Is(err, agent.ErrIdempotencyConflict) {
			t.Fatalf("conflicting retry after committed-unknown = %v", err)
		}
	})

	t.Run("lost_unknown_create_fails_then_exact_retry_succeeds", func(t *testing.T) {
		service, wrapper := openCommitUnknownAgent(t, factory)
		contextRef, taskRef := commitUnknownRefs("task-lost")
		create := commitUnknownCreate(contextRef, taskRef, "cmd-create", "start")
		wrapper.ArmLostUnknown()
		if _, err := service.CreateTask(t.Context(), create); !errors.Is(err, agent.ErrStoreCommitUnknown) {
			t.Fatalf("lost commit-unknown create = %v, want ErrStoreCommitUnknown", err)
		}
		task, err := service.CreateTask(t.Context(), create)
		if err != nil {
			t.Fatalf("exact retry after lost commit: %v", err)
		}
		if task.State != agent.TaskSubmitted || task.Revision != 1 {
			t.Fatalf("retried task = %#v", task)
		}
		assertSingleCommitUnknownTask(t, service, contextRef)
	})

	t.Run("committed_unknown_worker_mutation_replays_receipt", func(t *testing.T) {
		service, wrapper := openCommitUnknownAgent(t, factory)
		contextRef, taskRef := commitUnknownRefs("task-worker")
		if _, err := service.CreateTask(t.Context(),
			commitUnknownCreate(contextRef, taskRef, "cmd-create", "start")); err != nil {
			t.Fatal(err)
		}
		assignment, err := service.AcquireLease(t.Context(), agent.AcquireLeaseCommand{
			ID: "cmd-lease", Task: taskRef, Worker: "worker-a",
		})
		if err != nil {
			t.Fatal(err)
		}
		accept := agent.WorkerTaskCommand{
			Meta: agent.WorkerCommandMeta{ID: "cmd-accept", ExpectedRevision: 1, Grant: assignment.Grant},
			Task: taskRef,
		}
		wrapper.ArmCommittedUnknown()
		if _, err := service.AcceptTask(t.Context(), accept); !errors.Is(err, agent.ErrStoreCommitUnknown) {
			t.Fatalf("commit-unknown accept = %v, want ErrStoreCommitUnknown", err)
		}
		first, err := service.AcceptTask(t.Context(), accept)
		if err != nil {
			t.Fatalf("exact worker retry did not replay the committed receipt: %v", err)
		}
		if first.Revision != 2 || first.EventCount != 2 {
			t.Fatalf("worker mutation advanced the Task twice: %#v", first)
		}
		second, err := service.AcceptTask(t.Context(), accept)
		if err != nil || !reflect.DeepEqual(second, first) {
			t.Fatalf("worker receipt replay differs:\n got %#v, %v\nwant %#v", second, err, first)
		}
	})
}

func openCommitUnknownAgent(
	t *testing.T,
	factory AgentStoreFactory,
) (*agent.Agent, *CommitUnknownAgentStore) {
	t.Helper()
	inner, release, err := factory(t.Context(), t)
	if err != nil {
		t.Fatalf("open Agent Store: %v", err)
	}
	if inner == nil || release == nil {
		t.Fatal("Agent Store factory returned nil store or release")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := release(ctx); err != nil {
			t.Errorf("release Agent Store: %v", err)
		}
	})
	wrapper := WrapCommitUnknownAgentStore(inner)
	config := agent.DefaultConfig()
	config.Store = framework.Borrow[agent.Store](wrapper)
	service, err := agent.Open(t.Context(), config)
	if err != nil {
		t.Fatalf("open Agent domain: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Errorf("close Agent domain: %v", err)
		}
	})
	return service, wrapper
}

func commitUnknownRefs(task string) (agent.ContextRef, agent.TaskRef) {
	authority := agent.AuthorityID("tenant-a")
	return agent.ContextRef{Authority: authority, ID: "conversation-1"}, agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: authority, ID: "workspace-1"},
		ID:        agent.TaskID(task),
	}
}

func commitUnknownCreate(
	contextRef agent.ContextRef,
	taskRef agent.TaskRef,
	command, text string,
) agent.CreateTaskCommand {
	return agent.CreateTaskCommand{
		Meta: agent.CommandMeta{ID: agent.CommandID(command)},
		Task: taskRef, Context: contextRef,
		Message: commitUnknownMessage("message-1", text),
	}
}

func commitUnknownMessage(id, text string) agent.MessageInput {
	return agent.MessageInput{
		ID:    agent.MessageID(id),
		Parts: []agent.Part{{MediaType: "text/plain", Data: []byte(text)}},
	}
}

func assertSingleCommitUnknownTask(t *testing.T, service *agent.Agent, contextRef agent.ContextRef) {
	t.Helper()
	page, err := service.ListTasks(t.Context(), contextRef, agent.TaskPageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("ambiguous commit produced %d tasks, want exactly 1", len(page.Items))
	}
}
