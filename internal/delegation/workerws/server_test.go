package workerws

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
)

type testAuthenticator struct{}

func (testAuthenticator) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	switch token {
	case "worker-one":
		return auth.Principal{Type: auth.PrincipalWorker, SubjectID: "worker-1"}, nil
	case "worker-two":
		return auth.Principal{Type: auth.PrincipalWorker, SubjectID: "worker-2"}, nil
	case "caller":
		return auth.Principal{Type: auth.PrincipalCaller, SubjectID: "caller-1"}, nil
	default:
		return auth.Principal{}, auth.ErrUnauthorized
	}
}

func TestSubmittedQueueIsOrderedAndTerminalTaskIsRemoved(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	server := serve(t, store)
	connection := dialWorker(t, server.URL, "worker-one")
	defer connection.CloseNow()
	readHello(t, connection, "worker-1")
	for _, taskID := range []string{"queue-a", "queue-b"} {
		if _, err := store.CreateTask(ctx, delegation.CreateTaskInput{ID: taskID, CallerID: "caller-1"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, taskID := range []string{"queue-a", "queue-b"} {
		snapshot := readType(t, connection, workerproto.MessageSnapshot, 5*time.Second)
		var queued workerproto.Snapshot
		if err := json.Unmarshal(snapshot.Payload, &queued); err != nil {
			t.Fatal(err)
		}
		if queued.Reason != workerproto.ReasonSubmitted || queued.Snapshot.Task.ID != taskID || queued.EventID == "" {
			t.Fatalf("queued snapshot = %#v", queued)
		}
	}
	writeCommand(t, connection, 1, workerproto.Command{
		EventID: "reject-queued", Kind: workerproto.CommandReject,
		TaskID: "queue-a", ExpectedRevision: 1,
	})
	readResult(t, connection, "reject-queued", 5*time.Second)
	envelope := readType(t, connection, workerproto.MessageTaskRemoved, 5*time.Second)
	var removed workerproto.TaskRemoved
	if err := json.Unmarshal(envelope.Payload, &removed); err != nil {
		t.Fatal(err)
	}
	if removed.TaskID != "queue-a" || removed.EventID == "" {
		t.Fatalf("removed = %#v", removed)
	}
}

func TestWorkerWebSocketAuthRecoveryReplayAndSequenceFaults(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	created, err := store.CreateTask(ctx, delegation.CreateTaskInput{ID: "task-1", CallerID: "caller-1"})
	if err != nil {
		t.Fatal(err)
	}
	server := serve(t, store)

	for name, token := range map[string]string{"missing": "", "caller": "caller", "invalid": "nope"} {
		t.Run("auth_"+name, func(t *testing.T) {
			_, response, err := websocket.Dial(ctx, wsURL(server.URL), &websocket.DialOptions{
				HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}},
			})
			if err == nil {
				t.Fatal("unauthorized dial succeeded")
			}
			if response == nil || response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("response = %#v, error = %v", response, err)
			}
		})
	}

	connection := dialWorker(t, server.URL, "worker-one")
	readHello(t, connection, "worker-1")
	recovery := readSnapshot(t, connection, "task-1", 2*time.Second)
	if recovery.Reason != workerproto.ReasonRecovery || recovery.Snapshot.Task.State != delegation.StateSubmitted {
		t.Fatalf("recovery = %#v", recovery)
	}
	command := workerproto.Command{
		EventID: "event-accept", Kind: workerproto.CommandAccept,
		TaskID: "task-1", ExpectedRevision: created.Task.Revision,
	}
	writeCommand(t, connection, 1, command)
	result := readResult(t, connection, command.EventID, 2*time.Second)
	if result.Error != nil || result.Replay || result.Transition == nil ||
		result.Transition.Task.WorkerID != "worker-1" || result.Transition.Task.Revision != 2 {
		t.Fatalf("accept result = %#v", result)
	}

	// Repeating a connection sequence is only ACKed and never re-executed.
	writeCommand(t, connection, 1, command)
	readType(t, connection, workerproto.MessageAck, 2*time.Second)
	if task, err := store.GetTask(ctx, "task-1"); err != nil || task.Revision != 2 {
		t.Fatalf("task after duplicate frame = %#v, %v", task, err)
	}
	_ = connection.Close(websocket.StatusNormalClosure, "reconnect")

	// A new connection restarts seq at 1, while event_id durably replays the
	// prior command result instead of running the CAS transition again.
	connection = dialWorker(t, server.URL, "worker-one")
	defer connection.CloseNow()
	readHello(t, connection, "worker-1")
	recovery = readSnapshot(t, connection, "task-1", 2*time.Second)
	if recovery.Snapshot.Task.Revision != 2 || recovery.Snapshot.Task.WorkerID != "worker-1" {
		t.Fatalf("reconnected recovery = %#v", recovery)
	}
	writeCommand(t, connection, 1, command)
	result = readResult(t, connection, command.EventID, 2*time.Second)
	if !result.Replay || result.Transition == nil || result.Transition.Task.Revision != 2 {
		t.Fatalf("replayed result = %#v", result)
	}

	changed := command
	changed.ExpectedRevision = 2
	writeCommand(t, connection, 2, changed)
	conflict := readResult(t, connection, changed.EventID, 2*time.Second)
	if conflict.Error == nil || conflict.Error.Code != "idempotency_conflict" {
		t.Fatalf("changed event result = %#v", conflict)
	}

	// seq 3 was never sent. A gap is a protocol fault and cannot mutate state.
	gap := command
	gap.EventID = "event-gap"
	gap.ExpectedRevision = 2
	writeCommand(t, connection, 4, gap)
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		var envelope workerproto.Envelope
		if err := wsjson.Read(readCtx, connection, &envelope); err != nil {
			break
		}
	}
	task, err := store.GetTask(ctx, "task-1")
	if err != nil || task.Revision != 2 || task.WorkerID != "worker-1" {
		t.Fatalf("task after gap = %#v, %v", task, err)
	}
	events, err := store.ListEvents(ctx, "task-1", 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("events after faults = %#v, %v", events, err)
	}
}

func TestWorkerExecUsesAuthenticatedIdentityAndDurableReplay(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	if _, err := store.CreateTask(ctx, delegation.CreateTaskInput{ID: "exec-task", CallerID: "caller-1"}); err != nil {
		t.Fatal(err)
	}
	server := serve(t, store)
	connection := dialWorker(t, server.URL, "worker-one")
	readHello(t, connection, "worker-1")
	readSnapshot(t, connection, "exec-task", 2*time.Second)

	writeCommand(t, connection, 1, workerproto.Command{
		EventID: "accept-exec-task", Kind: workerproto.CommandAccept,
		TaskID: "exec-task", ExpectedRevision: 1,
	})
	accepted := readResult(t, connection, "accept-exec-task", 2*time.Second)
	if accepted.Error != nil || accepted.Transition == nil {
		t.Fatalf("accept result = %#v", accepted)
	}
	execCommand := workerproto.Command{
		EventID: "exec-event-1", Kind: workerproto.CommandExec,
		TaskID: "exec-task", ExpectedRevision: accepted.Transition.Task.Revision,
		Exec: &workerproto.ExecRequest{
			RequestID: "exec-request-1", Command: "go test ./...", CWD: ".",
			TimeoutMS: 45_000, Reason: "verify the delegated change",
		},
	}
	writeCommand(t, connection, 2, execCommand)
	requested := readResult(t, connection, execCommand.EventID, 2*time.Second)
	if requested.Error != nil || requested.Exec == nil || requested.Replay || requested.Exec.Replay {
		t.Fatalf("exec result = %#v", requested)
	}
	if requested.Exec.Request.WorkerID != "worker-1" || requested.Exec.Request.ID != "exec-request-1" ||
		requested.Exec.Request.Status != delegation.ExecPending || requested.Exec.Task.State != delegation.StateWorking ||
		requested.Exec.Task.Revision != 3 || requested.Exec.Event.Kind != delegation.EventExecRequested {
		t.Fatalf("authenticated exec result = %#v", requested.Exec)
	}
	stored, err := store.ListExecRequests(ctx, "exec-task")
	if err != nil || len(stored) != 1 || stored[0].WorkerID != "worker-1" {
		t.Fatalf("stored exec requests = %#v, error = %v", stored, err)
	}
	task, err := store.GetTask(ctx, "exec-task")
	if err != nil || task.State != delegation.StateWorking || task.Revision != 3 {
		t.Fatalf("task after exec request = %#v, error = %v", task, err)
	}
	snapshot, err := store.LoadSnapshot(ctx, "exec-task")
	if err != nil || snapshotReason(snapshot, false) != workerproto.ReasonExec || len(snapshot.Exec) != 1 {
		t.Fatalf("exec snapshot = %#v, error = %v", snapshot, err)
	}

	// Transport event replay survives reconnect and returns the persisted
	// receipt without advancing either the task clock or the exec lifecycle.
	_ = connection.Close(websocket.StatusNormalClosure, "replay exec")
	connection = dialWorker(t, server.URL, "worker-one")
	defer connection.CloseNow()
	readHello(t, connection, "worker-1")
	readSnapshotRevision(t, connection, "exec-task", 3, 2*time.Second)
	writeCommand(t, connection, 1, execCommand)
	replayedReceipt := readResult(t, connection, execCommand.EventID, 2*time.Second)
	if !replayedReceipt.Replay || replayedReceipt.Exec == nil || replayedReceipt.Exec.Task.Revision != 3 {
		t.Fatalf("replayed exec receipt = %#v", replayedReceipt)
	}

	// A distinct transport event with the same request_id is a domain replay.
	// Its stale CAS is intentionally harmless because the exact request already
	// exists in SQLite.
	domainReplay := execCommand
	domainReplay.EventID = "exec-event-2"
	writeCommand(t, connection, 2, domainReplay)
	replayedRequest := readResult(t, connection, domainReplay.EventID, 2*time.Second)
	if replayedRequest.Replay || replayedRequest.Exec == nil || !replayedRequest.Exec.Replay ||
		replayedRequest.Exec.Task.Revision != 3 {
		t.Fatalf("replayed exec request = %#v", replayedRequest)
	}
	stored, err = store.ListExecRequests(ctx, "exec-task")
	if err != nil || len(stored) != 1 {
		t.Fatalf("exec requests after replays = %#v, error = %v", stored, err)
	}

	other := dialWorker(t, server.URL, "worker-two")
	defer other.CloseNow()
	readHello(t, other, "worker-2")
	writeCommand(t, other, 1, workerproto.Command{
		EventID: "exec-event-other", Kind: workerproto.CommandExec,
		TaskID: "exec-task", ExpectedRevision: 3,
		Exec: &workerproto.ExecRequest{
			RequestID: "exec-request-other", Command: "pwd", Reason: "inspect workspace",
		},
	})
	ownership := readResult(t, other, "exec-event-other", 2*time.Second)
	if ownership.Error == nil || ownership.Error.Code != "ownership_conflict" || ownership.Exec != nil {
		t.Fatalf("cross-worker exec result = %#v", ownership)
	}
}

func TestWorkerRemoteExecIsDefaultClosed(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	if _, err := store.CreateTask(ctx, delegation.CreateTaskInput{ID: "closed-exec", CallerID: "caller-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcceptTask(ctx, delegation.AcceptTaskInput{
		CommandInput: delegation.CommandInput{TaskID: "closed-exec", ExpectedRevision: 1},
		WorkerID:     "worker-1",
	}); err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{}, testAuthenticator{}, store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := server.executeCommand(ctx, "worker-1", workerproto.Command{
		EventID: "closed-event", Kind: workerproto.CommandExec,
		TaskID: "closed-exec", ExpectedRevision: 2,
		Exec: &workerproto.ExecRequest{
			RequestID: "closed-request", Command: "pwd", Reason: "inspect",
		},
	})
	if err != nil || result.Error == nil || result.Error.Code != "invalid_input" {
		t.Fatalf("default-closed exec result = %#v, error = %v", result, err)
	}
	requests, listErr := store.ListExecRequests(ctx, "closed-exec")
	if listErr != nil || len(requests) != 0 {
		t.Fatalf("default-closed exec persisted requests = %#v, %v", requests, listErr)
	}
}

func TestWorkerSnapshotsClassifyInterruptRewindAndReconnectRecovery(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	if _, err := store.CreateTask(ctx, delegation.CreateTaskInput{ID: "task-events", CallerID: "caller-1"}); err != nil {
		t.Fatal(err)
	}
	server := serve(t, store)
	connection := dialWorker(t, server.URL, "worker-one")
	readHello(t, connection, "worker-1")
	readSnapshot(t, connection, "task-events", 2*time.Second)

	writeCommand(t, connection, 1, workerproto.Command{
		EventID: "accept-events", Kind: workerproto.CommandAccept,
		TaskID: "task-events", ExpectedRevision: 1,
	})
	readResult(t, connection, "accept-events", 2*time.Second)
	delivered, err := store.DeliverTurn(ctx, delegation.DeliverTurnInput{
		CommandInput: delegation.CommandInput{TaskID: "task-events", ExpectedRevision: 2},
		ArtifactID:   "artifact-1", ArtifactMediaType: delegation.GitPatchMediaType,
		ArtifactData: []byte("patch"),
	})
	if err != nil {
		t.Fatal(err)
	}
	interruptData := []byte(`{"messageId":"interrupt-1","metadata":{"intent":"interrupt"}}`)
	interrupted, err := store.AppendMessage(ctx,
		delegation.CommandInput{TaskID: "task-events", ExpectedRevision: delivered.Task.Revision},
		delegation.MessageInput{ID: "interrupt-1", Role: "user", Data: interruptData})
	if err != nil {
		t.Fatal(err)
	}
	interrupt := readSnapshotRevision(t, connection, "task-events", interrupted.Task.Revision, 2*time.Second)
	if interrupt.Reason != workerproto.ReasonInterrupt {
		t.Fatalf("interrupt reason = %q", interrupt.Reason)
	}
	requested, err := store.RequestRewind(ctx, delegation.RequestRewindInput{
		CommandInput: delegation.CommandInput{TaskID: "task-events", ExpectedRevision: interrupted.Task.Revision},
		TargetTurn:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	rewind := readSnapshotRevision(t, connection, "task-events", requested.Task.Revision, 2*time.Second)
	if rewind.Reason != workerproto.ReasonRewind {
		t.Fatalf("rewind reason = %q", rewind.Reason)
	}
	_ = connection.Close(websocket.StatusNormalClosure, "test recovery")

	normalData := []byte(`{"messageId":"message-2","metadata":{"intent":"message"}}`)
	appended, err := store.AppendMessage(ctx,
		delegation.CommandInput{TaskID: "task-events", ExpectedRevision: requested.Task.Revision},
		delegation.MessageInput{ID: "message-2", Role: "user", Data: normalData})
	if err != nil {
		t.Fatal(err)
	}
	connection = dialWorker(t, server.URL, "worker-one")
	defer connection.CloseNow()
	readHello(t, connection, "worker-1")
	recovered := readSnapshotRevision(t, connection, "task-events", appended.Task.Revision, 2*time.Second)
	if recovered.Reason != workerproto.ReasonRecovery || len(recovered.Snapshot.Messages) != 2 {
		t.Fatalf("recovered snapshot = %#v", recovered)
	}
}

func TestWorkerOwnershipAndLifecycleCommands(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	for _, taskID := range []string{"reject", "fail", "complete", "confirm", "reject-rewind"} {
		if _, err := store.CreateTask(ctx, delegation.CreateTaskInput{ID: taskID, CallerID: "caller-1"}); err != nil {
			t.Fatal(err)
		}
	}
	server := serve(t, store)
	connection := dialWorker(t, server.URL, "worker-one")
	defer connection.CloseNow()
	readHello(t, connection, "worker-1")
	for range 5 {
		readType(t, connection, workerproto.MessageSnapshot, 2*time.Second)
	}
	var seq uint64
	do := func(command workerproto.Command) workerproto.CommandResult {
		seq++
		writeCommand(t, connection, seq, command)
		return readResult(t, connection, command.EventID, 2*time.Second)
	}
	assertOK := func(result workerproto.CommandResult) {
		if result.Error != nil {
			t.Fatalf("command failed: %#v", result)
		}
	}

	assertOK(do(workerproto.Command{EventID: "reject-1", Kind: workerproto.CommandReject, TaskID: "reject", ExpectedRevision: 1}))
	assertOK(do(workerproto.Command{EventID: "accept-fail", Kind: workerproto.CommandAccept, TaskID: "fail", ExpectedRevision: 1}))
	assertOK(do(workerproto.Command{EventID: "fail-1", Kind: workerproto.CommandFail, TaskID: "fail", ExpectedRevision: 2}))
	assertOK(do(workerproto.Command{EventID: "accept-complete", Kind: workerproto.CommandAccept, TaskID: "complete", ExpectedRevision: 1}))
	assertOK(do(workerproto.Command{EventID: "complete-1", Kind: workerproto.CommandComplete, TaskID: "complete", ExpectedRevision: 2}))

	for _, taskID := range []string{"confirm", "reject-rewind"} {
		assertOK(do(workerproto.Command{EventID: "accept-" + taskID, Kind: workerproto.CommandAccept, TaskID: taskID, ExpectedRevision: 1}))
		assertOK(do(workerproto.Command{
			EventID: "deliver-" + taskID, Kind: workerproto.CommandDeliver,
			TaskID: taskID, ExpectedRevision: 2,
			Delivery: &workerproto.Delivery{
				ArtifactID: "artifact-" + taskID, ArtifactMediaType: delegation.GitPatchMediaType,
				ArtifactData: []byte("patch"),
			},
		}))
		replied, err := store.AppendMessage(ctx,
			delegation.CommandInput{TaskID: taskID, ExpectedRevision: 3},
			delegation.MessageInput{ID: "reply-" + taskID, Role: "user", Data: []byte(`{"text":"continue"}`)})
		if err != nil {
			t.Fatal(err)
		}
		requested, err := store.RequestRewind(ctx, delegation.RequestRewindInput{
			CommandInput: delegation.CommandInput{TaskID: taskID, ExpectedRevision: replied.Task.Revision},
			TargetTurn:   0,
		})
		if err != nil {
			t.Fatal(err)
		}
		kind := workerproto.CommandConfirmRewind
		if taskID == "reject-rewind" {
			kind = workerproto.CommandRejectRewind
		}
		assertOK(do(workerproto.Command{
			EventID: "resolve-" + taskID, Kind: kind,
			TaskID: taskID, ExpectedRevision: requested.Task.Revision,
		}))
	}

	// worker-2 can see submitted tasks but cannot mutate worker-1's assignment.
	other := dialWorker(t, server.URL, "worker-two")
	defer other.CloseNow()
	readHello(t, other, "worker-2")
	writeCommand(t, other, 1, workerproto.Command{
		EventID: "steal-complete", Kind: workerproto.CommandComplete,
		TaskID: "confirm", ExpectedRevision: 6,
	})
	result := readResult(t, other, "steal-complete", 2*time.Second)
	if result.Error == nil || result.Error.Code != "ownership_conflict" {
		t.Fatalf("ownership result = %#v", result)
	}
}

func openStore(t *testing.T) *delegation.Store {
	t.Helper()
	store, err := delegation.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "delegation.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func serve(t *testing.T, store *delegation.Store) *httptest.Server {
	t.Helper()
	handler, err := New(Config{SnapshotPoll: 10 * time.Millisecond, RemoteExec: true}, testAuthenticator{}, store)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func wsURL(httpURL string) string { return "ws" + strings.TrimPrefix(httpURL, "http") }

func dialWorker(t *testing.T, serverURL, token string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, wsURL(serverURL), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func writeCommand(t *testing.T, connection *websocket.Conn, sequence uint64, command workerproto.Command) {
	t.Helper()
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, connection, workerproto.Envelope{
		Version: workerproto.Version, Seq: sequence, Type: workerproto.MessageCommand, Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func readHello(t *testing.T, connection *websocket.Conn, workerID string) {
	t.Helper()
	envelope := readType(t, connection, workerproto.MessageHello, 2*time.Second)
	var hello workerproto.Hello
	if err := json.Unmarshal(envelope.Payload, &hello); err != nil || hello.WorkerID != workerID {
		t.Fatalf("hello = %#v, %v", hello, err)
	}
}

func readSnapshot(t *testing.T, connection *websocket.Conn, taskID string, timeout time.Duration) workerproto.Snapshot {
	t.Helper()
	return readSnapshotRevision(t, connection, taskID, 0, timeout)
}

func readSnapshotRevision(t *testing.T, connection *websocket.Conn, taskID string, revision int64, timeout time.Duration) workerproto.Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		envelope := readAny(t, connection, time.Until(deadline))
		if envelope.Type != workerproto.MessageSnapshot {
			continue
		}
		var snapshot workerproto.Snapshot
		if err := json.Unmarshal(envelope.Payload, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.Snapshot.Task.ID == taskID && (revision == 0 || snapshot.Snapshot.Task.Revision == revision) {
			return snapshot
		}
	}
	t.Fatalf("snapshot %q revision %d not received", taskID, revision)
	return workerproto.Snapshot{}
}

func readResult(t *testing.T, connection *websocket.Conn, eventID string, timeout time.Duration) workerproto.CommandResult {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		envelope := readAny(t, connection, time.Until(deadline))
		if envelope.Type != workerproto.MessageCommandResult {
			continue
		}
		var result workerproto.CommandResult
		if err := json.Unmarshal(envelope.Payload, &result); err != nil {
			t.Fatal(err)
		}
		if result.EventID == eventID {
			return result
		}
	}
	t.Fatalf("result %q not received", eventID)
	return workerproto.CommandResult{}
}

func readType(t *testing.T, connection *websocket.Conn, messageType workerproto.MessageType, timeout time.Duration) workerproto.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		envelope := readAny(t, connection, time.Until(deadline))
		if envelope.Type == messageType {
			return envelope
		}
	}
	t.Fatalf("message type %q not received", messageType)
	return workerproto.Envelope{}
}

func readAny(t *testing.T, connection *websocket.Conn, timeout time.Duration) workerproto.Envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var envelope workerproto.Envelope
	if err := wsjson.Read(ctx, connection, &envelope); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read websocket: %v", err)
		}
		t.Fatal(err)
	}
	return envelope
}
