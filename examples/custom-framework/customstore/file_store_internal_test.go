package customstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

func TestAmbiguousDirectorySyncPoisonsHandleUntilReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ambiguous.snapshot")
	resource, err := Open(t.Context(), internalConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	store, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected directory sync failure")
	store.(*fileStore).syncDirectory = func(string) error { return injected }
	err = store.Bind(t.Context(), llm.StoreBinding{DeploymentID: "deployment-ambiguous"})
	if !errors.Is(err, llm.ErrStoreCommitUnknown) || !errors.Is(err, ErrStorePoisoned) ||
		!errors.Is(err, injected) {
		t.Fatalf("ambiguous Bind error = %v", err)
	}
	called := false
	err = store.View(t.Context(), func(llm.StoreView) error {
		called = true
		return nil
	})
	if called || !errors.Is(err, llm.ErrStoreCommitUnknown) || !errors.Is(err, ErrStorePoisoned) {
		t.Fatalf("poisoned View calls/error = %v/%v", called, err)
	}
	if err := resource.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), internalConfig(path))
	if err != nil {
		t.Fatalf("reconcile poisoned handle by reopen: %v", err)
	}
	defer reopened.Release(context.Background())
	reopenedStore, err := reopened.Value()
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedStore.Bind(t.Context(), llm.StoreBinding{DeploymentID: "deployment-ambiguous"}); err != nil {
		t.Fatalf("ambiguous commit was not recoverable by exact replay: %v", err)
	}
}

func TestLoadRejectsChecksummedInvalidLogicalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.snapshot")
	state := newCustomState()
	state.tasks[llm.StoreTaskKey{Caller: "caller", Task: "task"}] = llm.StoreTaskRecord{
		Key:            llm.StoreTaskKey{Caller: "caller", Task: "task"},
		CapabilityTier: llm.TierChat,
		State:          "invalid-state",
		Revision:       1,
		CreatedAt:      time.Unix(1, 0).UTC(),
		UpdatedAt:      time.Unix(1, 0).UTC(),
	}
	encoded, err := encodeSnapshot(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), internalConfig(path)); !errors.Is(err, llm.ErrStoreCorruptRecord) ||
		!errors.Is(err, llm.ErrStoreInvalidArgument) {
		t.Fatalf("open checksummed invalid record = %v, want corrupt + invalid", err)
	}
}

func TestLoadRejectsChecksummedCrossRecordUniquenessViolations(t *testing.T) {
	tests := []struct {
		name       string
		constraint llm.StoreConstraint
		state      func() customState
	}{
		{
			name: "open affinity", constraint: llm.StoreConstraintOpenAffinity,
			state: func() customState {
				state := newCustomState()
				first := validInternalTask("first")
				second := validInternalTask("second")
				second.WorkspaceKey = first.WorkspaceKey
				second.HarnessID = first.HarnessID
				second.HarnessVersion = first.HarnessVersion
				second.HarnessSessionID = first.HarnessSessionID
				state.tasks[first.Key], state.tasks[second.Key] = first, second
				return state
			},
		},
		{
			name: "active request", constraint: llm.StoreConstraintActiveRequest,
			state: func() customState {
				state := newCustomState()
				task := validInternalTask("request-owner")
				first := validInternalRequest(task, "first")
				second := validInternalRequest(task, "second")
				state.tasks[task.Key] = task
				state.requests[first.Key], state.requests[second.Key] = first, second
				return state
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid-uniqueness.snapshot")
			encoded, err := encodeSnapshot(nil, test.state())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = Open(t.Context(), internalConfig(path))
			var conflict *llm.StoreConflictError
			if !errors.Is(err, llm.ErrStoreCorruptRecord) || !errors.As(err, &conflict) ||
				conflict.Constraint != test.constraint {
				t.Fatalf("open checksummed uniqueness violation = %v, want corrupt + %s", err, test.constraint)
			}
		})
	}
}

func internalConfig(path string) Config {
	return Config{Path: path, Provider: "example.custom-store", Version: "1"}
}

func validInternalTask(id string) llm.StoreTaskRecord {
	created := time.Unix(1, 0).UTC()
	return llm.StoreTaskRecord{
		Key:          llm.StoreTaskKey{Caller: "caller", Task: llm.TaskID("task-" + id)},
		WorkspaceKey: "workspace-" + id, CapabilityTier: llm.TierWorkspace,
		Codec: llm.CodecSnapshot{
			Contract: framework.Contract{ID: llm.CodecContractID, Major: llm.CodecContractMajor},
			ID:       "example.codec", Version: "1", Fingerprint: llm.Fingerprint([]byte("example-codec")),
		},
		HarnessID: "harness", HarnessVersion: "1", HarnessSessionID: "session-" + id,
		WorkspaceRoot: "/workspace", State: llm.TaskAwaitingCaller,
		Revision: 1, CreatedAt: created, UpdatedAt: created,
	}
}

func validInternalRequest(task llm.StoreTaskRecord, id string) llm.StoreRequestRecord {
	return llm.StoreRequestRecord{
		Key:  llm.StoreRequestKey{Caller: task.Key.Caller, IdempotencyKey: llm.IdempotencyKey("request-" + id)},
		Task: task.Key, RequestID: "request-id-" + id, ResponseID: "response-id-" + id,
		RequestDigest: llm.StoreDigest("digest-" + id), Codec: task.Codec,
		Mode: llm.ResponseStream, Revision: 1, CreatedAt: time.Unix(2, 0).UTC(),
	}
}
