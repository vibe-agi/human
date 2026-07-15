package workerclient

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/worker"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
	"github.com/vibe-agi/human/internal/delegation/workerws"
)

type integrationAuth struct{}

func (integrationAuth) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	if token != "worker-token" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return auth.Principal{Type: auth.PrincipalWorker, SubjectID: "worker-1"}, nil
}

func TestClientAuthorityCommandsInterruptAndReconnectRecovery(t *testing.T) {
	ctx := context.Background()
	store, err := delegation.OpenSQLite(ctx, filepath.Join(t.TempDir(), "delegation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTask(ctx, delegation.CreateTaskInput{ID: "task-1", CallerID: "caller-1"}); err != nil {
		t.Fatal(err)
	}
	handler, err := workerws.New(workerws.Config{SnapshotPoll: 10 * time.Millisecond}, integrationAuth{}, store)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := Dial(ctx, Config{
		URL: "ws" + strings.TrimPrefix(server.URL, "http"), Token: "worker-token",
		OutboxPath:       filepath.Join(t.TempDir(), "outbox.db"),
		ReconnectInitial: 10 * time.Millisecond, ReconnectMaximum: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	select {
	case <-client.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive hello")
	}
	waitCachedRevision(t, client, "task-1", 1, 2*time.Second)

	accepted, err := client.AcceptTask(ctx, delegation.AcceptTaskInput{
		CommandInput: delegation.CommandInput{TaskID: "task-1", ExpectedRevision: 1},
		WorkerID:     "worker-1",
	})
	if err != nil || accepted.Task.State != delegation.StateWorking || accepted.Task.WorkerID != "worker-1" {
		t.Fatalf("accepted = %#v, error = %v", accepted, err)
	}
	delivered, err := client.DeliverTurn(ctx, delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{TaskID: "task-1", ExpectedRevision: 2},
		ArtifactID:   "artifact-1", ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData: []byte("patch bytes"), ArtifactMetadata: []byte(`{"turn":1}`),
	})
	if err != nil || delivered.Task.State != delegation.StateInputRequired || delivered.Artifact.ID != "artifact-1" {
		t.Fatalf("delivered = %#v, error = %v", delivered, err)
	}
	artifact, err := client.GetArtifact(ctx, "task-1", 1)
	if err != nil || string(artifact.Data) != "patch bytes" {
		t.Fatalf("cached artifact = %#v, error = %v", artifact, err)
	}

	interruptData := []byte(`{"messageId":"interrupt-1","metadata":{"intent":"interrupt"}}`)
	interrupted, err := store.AppendMessage(ctx,
		delegation.CommandInput{TaskID: "task-1", ExpectedRevision: 3},
		delegation.MessageInput{ID: "interrupt-1", Role: "user", Data: interruptData})
	if err != nil {
		t.Fatal(err)
	}
	update := waitUpdateSnapshot(t, client, "task-1", interrupted.Task.Revision, 2*time.Second)
	if update.Reason != workerproto.ReasonInterrupt {
		t.Fatalf("interrupt reason = %q", update.Reason)
	}

	// Force a real socket break, mutate SQLite while disconnected, then verify
	// the reconnect starts with an authoritative recovery snapshot.
	client.writeMu.Lock()
	connection := client.connection
	client.writeMu.Unlock()
	if connection == nil {
		t.Fatal("client has no active connection")
	}
	_ = connection.CloseNow()
	requested, err := store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{TaskID: "task-1", ExpectedRevision: interrupted.Task.Revision},
		TargetTurn:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered := waitUpdateSnapshot(t, client, "task-1", requested.Task.Revision, 3*time.Second)
	if recovered.Reason != workerproto.ReasonRecovery || recovered.Snapshot.Task.PendingRewindTo == nil {
		t.Fatalf("recovery = %#v", recovered)
	}
	confirmed, err := client.ConfirmRewind(ctx, delegation.CommandInput{
		TaskID: "task-1", ExpectedRevision: requested.Task.Revision,
	})
	if err != nil || confirmed.Task.State != delegation.StateInputRequired || confirmed.Task.LatestTurn != 0 {
		t.Fatalf("confirmed = %#v, error = %v", confirmed, err)
	}
}

func TestClientMapsCASAndOwnershipErrors(t *testing.T) {
	if !errors.Is(decodeProtocolError(&workerproto.Error{Code: "revision_conflict", Message: "stale"}), delegation.ErrRevisionConflict) {
		t.Fatal("revision conflict sentinel was not preserved")
	}
	if !errors.Is(decodeProtocolError(&workerproto.Error{Code: "already_exists", Message: "duplicate"}), delegation.ErrAlreadyExists) {
		t.Fatal("already exists sentinel was not preserved")
	}
	if !errors.Is(decodeProtocolError(&workerproto.Error{Code: "ownership_conflict", Message: "other"}), ErrOwnershipConflict) {
		t.Fatal("ownership conflict sentinel was not preserved")
	}
}

func waitCachedRevision(t *testing.T, client *Client, taskID string, revision int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task, err := client.GetTask(context.Background(), taskID)
		if err == nil && task.Revision == revision {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("task %q revision %d was not cached", taskID, revision)
}

func waitUpdateSnapshot(t *testing.T, client *Client, taskID string, revision int64, timeout time.Duration) workerproto.Snapshot {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			t.Fatalf("task %q revision %d update not received", taskID, revision)
		case update, open := <-client.Updates():
			if !open {
				t.Fatal("client updates closed")
			}
			if update.Snapshot != nil && update.Snapshot.Snapshot.Task.ID == taskID &&
				update.Snapshot.Snapshot.Task.Revision == revision {
				return *update.Snapshot
			}
		}
	}
}

var _ worker.Authority = (*Client)(nil)
