package humantest

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

// WorkerStateStoreFactory opens a new, empty workerkit StateStore for one
// conformance subtest. Every invocation must return an independent store;
// release must be non-nil and is called exactly once with a fresh context.
type WorkerStateStoreFactory func(
	context.Context,
	testing.TB,
) (workerkit.StateStore, framework.ReleaseFunc, error)

// TestWorkerStateStore runs the black-box conformance suite for a workerkit
// StateStore: exact whole-record roundtrip, value ownership across the store
// boundary, deterministic listing order, upsert semantics, idempotent delete,
// and context validation. Durable adapters must additionally prove
// reopen-after-release recovery in their own tests; this suite is
// storage-agnostic and never restarts the store.
func TestWorkerStateStore(t *testing.T, factory WorkerStateStoreFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("workerkit StateStore conformance factory is nil")
	}

	open := func(t *testing.T) workerkit.StateStore {
		t.Helper()
		store, release, err := factory(t.Context(), t)
		if err != nil {
			t.Fatalf("open workerkit StateStore: %v", err)
		}
		if store == nil || release == nil {
			t.Fatal("factory returned nil store or release")
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := release(ctx); err != nil {
				t.Errorf("release workerkit StateStore: %v", err)
			}
		})
		return store
	}

	t.Run("roundtrip_is_exact", func(t *testing.T) {
		store := open(t)
		saved := workerStateConversation("caller-a", "task-1")
		if err := store.SaveConversation(t.Context(), saved); err != nil {
			t.Fatal(err)
		}
		listed, err := store.ListConversations(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(listed) != 1 || !reflect.DeepEqual(normalizeConversation(listed[0]), normalizeConversation(saved)) {
			t.Fatalf("roundtrip differs:\n got %#v\nwant %#v", listed, saved)
		}
	})

	t.Run("values_do_not_alias_store_memory", func(t *testing.T) {
		store := open(t)
		saved := workerStateConversation("caller-a", "task-1")
		if err := store.SaveConversation(t.Context(), saved); err != nil {
			t.Fatal(err)
		}
		// Mutating the caller's copy after save must not change the store.
		saved.Transcript[0].Text = "mutated-after-save"
		saved.ParkedCalls[0].Input["command"] = "mutated"
		first, err := store.ListConversations(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if first[0].Transcript[0].Text == "mutated-after-save" ||
			first[0].ParkedCalls[0].Input["command"] == "mutated" {
			t.Fatal("store aliased caller memory on save")
		}
		// Mutating a listed copy must not change later reads.
		first[0].Transcript[0].Text = "mutated-after-list"
		second, err := store.ListConversations(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if second[0].Transcript[0].Text == "mutated-after-list" {
			t.Fatal("store returned aliased memory from list")
		}
	})

	t.Run("listing_is_ordered_and_upsert_replaces", func(t *testing.T) {
		store := open(t)
		for _, key := range []struct{ caller, task string }{
			{"caller-b", "task-2"}, {"caller-a", "task-9"}, {"caller-a", "task-1"},
		} {
			if err := store.SaveConversation(t.Context(), workerStateConversation(key.caller, key.task)); err != nil {
				t.Fatal(err)
			}
		}
		updated := workerStateConversation("caller-a", "task-1")
		updated.Phase = workerkit.PhaseTerminal
		updated.Draft = "revised"
		if err := store.SaveConversation(t.Context(), updated); err != nil {
			t.Fatal(err)
		}
		listed, err := store.ListConversations(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(listed) != 3 {
			t.Fatalf("listed %d conversations, want 3", len(listed))
		}
		wantOrder := []workerkit.ConversationKey{
			{Caller: "caller-a", TaskID: "task-1"},
			{Caller: "caller-a", TaskID: "task-9"},
			{Caller: "caller-b", TaskID: "task-2"},
		}
		for index, want := range wantOrder {
			if listed[index].Key != want {
				t.Fatalf("order[%d] = %v, want %v", index, listed[index].Key, want)
			}
		}
		if listed[0].Phase != workerkit.PhaseTerminal || listed[0].Draft != "revised" {
			t.Fatalf("upsert did not replace: %#v", listed[0])
		}
	})

	t.Run("delete_is_idempotent", func(t *testing.T) {
		store := open(t)
		key := workerkit.ConversationKey{Caller: "caller-a", TaskID: "task-1"}
		if err := store.SaveConversation(t.Context(), workerStateConversation("caller-a", "task-1")); err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteConversation(t.Context(), key); err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteConversation(t.Context(), key); err != nil {
			t.Fatalf("second delete = %v, want no-op", err)
		}
		if err := store.DeleteConversation(t.Context(), workerkit.ConversationKey{
			Caller: "caller-x", TaskID: "task-x",
		}); err != nil {
			t.Fatalf("deleting an absent key = %v, want no-op", err)
		}
		listed, err := store.ListConversations(t.Context())
		if err != nil || len(listed) != 0 {
			t.Fatalf("after delete: %v, %v", listed, err)
		}
	})

	t.Run("contexts_are_validated", func(t *testing.T) {
		store := open(t)
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := store.SaveConversation(canceled, workerStateConversation("caller-a", "task-1")); err == nil {
			t.Fatal("save with canceled context succeeded")
		}
		if _, err := store.ListConversations(canceled); err == nil {
			t.Fatal("list with canceled context succeeded")
		}
	})
}

func workerStateConversation(caller, task string) workerkit.Conversation {
	at := time.Unix(1_800_000_000, 0).UTC()
	return workerkit.Conversation{
		Key:   workerkit.ConversationKey{Caller: llm.CallerID(caller), TaskID: llm.TaskID(task)},
		Phase: workerkit.PhaseAwaitingResults,
		Assignment: llm.WorkerAssignmentDelivery{
			ID: "delivery-1",
			Assignment: llm.Assignment{
				Identity: llm.CompletionIdentity{
					CallerID: llm.CallerID(caller), RequestID: "request-1",
					TaskID: llm.TaskID(task), IdempotencyKey: "turn-1",
				},
				Lease:    llm.WorkerLease{ID: "lease-1", Owner: "worker-a"},
				Boundary: llm.AssignmentAfterResponse,
				Task: llm.TaskContext{
					TaskID: llm.TaskID(task), CapabilityTier: llm.TierWorkspace,
					WorkspaceKey: "workspace-a", HarnessID: "harness-a", HarnessVersion: "v1",
					HarnessSessionID: "session-a", WorkspaceRoot: "/workspace",
				},
				Request: llm.Request{
					Model: "human", Stream: true,
					Messages: []llm.Message{{
						Role: llm.RoleUser, Blocks: []llm.Block{{Type: llm.BlockText, Text: "seed"}},
					}},
				},
			},
		},
		Transcript: []workerkit.TranscriptEntry{
			{At: at, Author: workerkit.AuthorCaller, Kind: workerkit.EntryText, Text: "seed"},
			{At: at, Author: workerkit.AuthorHuman, Kind: workerkit.EntryToolCalls,
				ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "bash", Input: map[string]any{"command": "ls"}}}},
		},
		ParkedCalls: []llm.ToolCall{{ID: "call-1", Name: "bash", Input: map[string]any{"command": "ls"}}},
		Draft:       "draft text",
		UpdatedAt:   at,
	}
}

// normalizeConversation flattens JSON-roundtrip-equivalent values (e.g.
// numeric types inside tool inputs) so DeepEqual compares semantics.
func normalizeConversation(conversation workerkit.Conversation) map[string]any {
	encoded, err := jsonRoundTrip(conversation)
	if err != nil {
		panic(err)
	}
	return encoded
}
