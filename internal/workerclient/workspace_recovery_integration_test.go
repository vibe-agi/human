package workerclient

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/mirror"
)

const (
	workspaceChaosKey          = "workspace-three-party-recovery"
	workspaceChaosFirstKey     = "workspace-edit-before-outage"
	workspaceChaosContinueKey  = "workspace-result-after-recovery"
	workspaceChaosSessionID    = "ses_workspace_three_party_recovery"
	workspaceChaosEditEventID  = "native-edit-during-three-party-outage"
	workspaceChaosAcceptID     = "workspace-accepted-before-outage"
	workspaceChaosContinueDone = "workspace-continuation-complete"
)

// TestWorkspaceToolLoopSurvivesThreePartyOutage exercises the product's
// correctness boundary rather than only a text completion: a reviewed Human
// mirror edit is durably queued while caller, gateway, and worker are all
// disconnected; gateway/SQLite and the worker outbox restart; same-key caller
// retries observe one native OpenCode edit; and the tool-result continuation
// advances the recorded v1 baseline exactly once while preserving a newer v2
// Human draft.
func TestWorkspaceToolLoopSurvivesThreePartyOutage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "gateway.db")
	outboxPath := filepath.Join(directory, "worker-outbox.db")
	callerRoot := filepath.Join(directory, "caller-workspace")
	if err := os.MkdirAll(callerRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	humanWorkspace, err := mirror.Open(
		filepath.Join(directory, "human-mirrors"), chaosCallerID, workspaceChaosKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	v0 := []byte("version zero from caller\n")
	if err := humanWorkspace.Hydrate("native.txt", v0, callerfs.Fingerprint(v0)); err != nil {
		t.Fatal(err)
	}
	v1 := []byte("version one sent by Human\n")
	if err := os.WriteFile(filepath.Join(humanWorkspace.Dir(), "native.txt"), v1, 0o600); err != nil {
		t.Fatal(err)
	}
	changes, err := humanWorkspace.Review()
	if err != nil || len(changes) != 1 {
		t.Fatalf("initial Human review = %+v, %v", changes, err)
	}
	profile := adapter.OpenCode11718Profile()
	delivery, err := mirror.BuildToolCallsForProfile(changes, &profile, callerRoot)
	if err != nil || len(delivery.Calls) != 1 || delivery.Calls[0].Name != "edit" {
		t.Fatalf("native OpenCode delivery = %+v, %v", delivery, err)
	}
	editCall := delivery.Calls[0]
	if err := humanWorkspace.RecordDeliveryIntents(changes, delivery.Calls, &profile, callerRoot); err != nil {
		t.Fatal(err)
	}
	// The operator saves v2 after v1 has crossed the local outbox boundary. A
	// delayed v1 result must advance to the sent bytes, not to this newer draft.
	v2 := []byte("version two remains a Human draft\n")
	if err := os.WriteFile(filepath.Join(humanWorkspace.Dir(), "native.txt"), v2, 0o600); err != nil {
		t.Fatal(err)
	}

	firstBody := workspaceOpenCodeBody(t, nil)
	continuationBody := workspaceOpenCodeBody(t, &editCall)
	daemon := startLiveDaemon(t, "127.0.0.1:0", databasePath)
	address := daemon.address
	caller := newChaosHTTPClient()
	defer caller.CloseIdleConnections()
	worker, err := DialWithOutbox(ctx, daemon.workerURL(), chaosWorkerToken, outboxPath)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return worker.WorkerID() == chaosWorkerID }, "initial Workspace worker hello")

	interrupted := make(chan callerAttempt, 1)
	go func() {
		interrupted <- postWorkspaceCompletion(
			caller, daemon.httpURL(), firstBody, workspaceChaosFirstKey, callerRoot,
		)
	}()
	assignment := receiveClientAssignment(t, ctx, worker.Messages())
	if assignment.CallerID != chaosCallerID || assignment.WorkspaceKey != workspaceChaosKey ||
		assignment.IdempotencyKey != workspaceChaosFirstKey ||
		assignment.CapabilityTier != completion.TierWorkspace || assignment.Adapter == nil ||
		assignment.Adapter.Key() != adapter.OpenCodeID+"@"+adapter.OpenCodeVersion ||
		assignment.Root != callerRoot || assignment.LeaseOwner != chaosWorkerID ||
		!strings.HasPrefix(assignment.TaskID, "opencode-task:v1:") {
		t.Fatalf("initial Workspace assignment = %+v", assignment)
	}
	if err := worker.SendEvent(ctx, assignment, completion.Event{
		ID: workspaceChaosAcceptID, Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(worker) == 0 }, "Workspace accepted ACK")

	// Overlap all three failures. The stream and worker TCP socket disappear,
	// then the native edit is committed only to the worker's on-disk outbox,
	// and finally the worker process and gateway/SQLite process both stop.
	daemon.cutNetwork(t)
	closeWorkerConnection(worker)
	waitFor(t, ctx, func() bool {
		worker.writeMu.Lock()
		defer worker.writeMu.Unlock()
		return worker.connection == nil
	}, "Workspace worker to observe gateway outage")
	select {
	case result := <-interrupted:
		if result.status != http.StatusOK || bytes.Contains(result.body, []byte(editCall.ID)) ||
			bytes.Contains(result.body, []byte("data: [DONE]")) {
			t.Fatalf("interrupted Workspace caller completed: status=%d body=%q err=%v", result.status, result.body, result.err)
		}
	case <-ctx.Done():
		t.Fatal("Workspace caller did not observe injected outage")
	}
	if err := worker.SendEvent(ctx, assignment, completion.Event{
		ID: workspaceChaosEditEventID, Type: completion.EventToolCalls,
		ToolCalls: []completion.ToolCall{editCall},
	}); err != nil {
		t.Fatal(err)
	}
	if count := pendingOutboxCount(worker); count != 1 {
		t.Fatalf("offline Workspace outbox count = %d, want 1", count)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
	daemon.closeCore(t)

	for attempt := 1; attempt <= 3; attempt++ {
		failed := postWorkspaceCompletion(
			caller, "http://"+address, firstBody, workspaceChaosFirstKey, callerRoot,
		)
		if failed.err == nil {
			t.Fatalf("offline Workspace retry %d returned status=%d body=%q", attempt, failed.status, failed.body)
		}
	}

	restarted := startLiveDaemon(t, address, databasePath)
	t.Cleanup(func() {
		_ = restarted.httpServer.Close()
		restarted.cancel()
		restarted.gateway.Wait()
		_ = restarted.database.Close()
	})
	const retryCount = 3
	retries := make(chan callerAttempt, retryCount)
	for index := 0; index < retryCount; index++ {
		go func() {
			retries <- postWorkspaceCompletion(
				caller, restarted.httpURL(), firstBody, workspaceChaosFirstKey, callerRoot,
			)
		}()
	}
	reopened, err := DialWithOutbox(ctx, restarted.workerURL(), chaosWorkerToken, outboxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	waitFor(t, ctx, func() bool { return reopened.WorkerID() == chaosWorkerID }, "reopened Workspace worker hello")
	recovered := receiveClientAssignment(t, ctx, reopened.Messages())
	if recovered.SessionKey() != assignment.SessionKey() || recovered.TaskID != assignment.TaskID ||
		recovered.CapabilityTier != completion.TierWorkspace {
		t.Fatalf("recovered Workspace assignment = %+v, initial = %+v", recovered, assignment)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(reopened) == 0 }, "offline native edit ACK")

	var firstReplay []byte
	for index := 0; index < retryCount; index++ {
		select {
		case result := <-retries:
			if result.err != nil || result.status != http.StatusOK ||
				bytes.Count(result.body, []byte(editCall.ID)) != 1 ||
				bytes.Count(result.body, []byte(`"name":"edit"`)) != 1 ||
				bytes.Count(result.body, []byte("data: [DONE]")) != 1 {
				t.Fatalf("recovered Workspace retry %d: status=%d body=%q err=%v", index+1, result.status, result.body, result.err)
			}
			if firstReplay == nil {
				firstReplay = result.body
			} else if !bytes.Equal(firstReplay, result.body) {
				t.Fatalf("same-key Workspace replay changed bytes\nfirst=%q\nnext=%q", firstReplay, result.body)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Workspace retry %d", index+1)
		}
	}

	continuationResult := make(chan callerAttempt, 1)
	go func() {
		continuationResult <- postWorkspaceCompletion(
			caller, restarted.httpURL(), continuationBody, workspaceChaosContinueKey, callerRoot,
		)
	}()
	continuation := receiveClientAssignment(t, ctx, reopened.Messages())
	if continuation.TaskID != assignment.TaskID || continuation.IdempotencyKey != workspaceChaosContinueKey ||
		continuation.SessionKey() == assignment.SessionKey() {
		t.Fatalf("Workspace continuation identity = %+v, initial = %+v", continuation, assignment)
	}
	reconciled, err := humanWorkspace.ReconcileRequestForProfile(
		continuation.Request, continuation.Adapter, callerRoot,
	)
	if err != nil || len(reconciled.Confirmed) != 1 || reconciled.Confirmed[0] != editCall.ID ||
		len(reconciled.Failed) != 0 {
		t.Fatalf("recovered native result reconciliation = %+v, %v", reconciled, err)
	}
	assertOnlyV2Remains(t, humanWorkspace, v1, v2)

	// Reopen the Human mirror and consume the same cumulative continuation a
	// second time. The result ledger must suppress another baseline advance.
	humanWorkspace, err = mirror.Open(
		filepath.Join(directory, "human-mirrors"), chaosCallerID, workspaceChaosKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	replayedResult, err := humanWorkspace.ReconcileRequestForProfile(
		continuation.Request, continuation.Adapter, callerRoot,
	)
	if err != nil || len(replayedResult.Confirmed) != 0 || len(replayedResult.Failed) != 0 {
		t.Fatalf("replayed native result reconciliation = %+v, %v", replayedResult, err)
	}
	assertOnlyV2Remains(t, humanWorkspace, v1, v2)

	if err := reopened.SendEvent(ctx, continuation, completion.Event{
		ID: "workspace-continuation-accepted", Type: completion.EventAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.SendEvent(ctx, continuation, completion.Event{
		ID: "workspace-continuation-final", Type: completion.EventFinal,
		Text: workspaceChaosContinueDone,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return pendingOutboxCount(reopened) == 0 }, "Workspace continuation ACKs")
	select {
	case result := <-continuationResult:
		if result.err != nil || result.status != http.StatusOK ||
			bytes.Count(result.body, []byte(workspaceChaosContinueDone)) != 1 ||
			bytes.Count(result.body, []byte("data: [DONE]")) != 1 {
			t.Fatalf("Workspace continuation response: status=%d body=%q err=%v", result.status, result.body, result.err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for Workspace continuation")
	}

	completedReplay := postWorkspaceCompletion(
		caller, restarted.httpURL(), continuationBody, workspaceChaosContinueKey, callerRoot,
	)
	if completedReplay.err != nil || completedReplay.status != http.StatusOK ||
		bytes.Count(completedReplay.body, []byte(workspaceChaosContinueDone)) != 1 {
		t.Fatalf("completed Workspace continuation replay = %d, %q, %v", completedReplay.status, completedReplay.body, completedReplay.err)
	}
	assertWorkspaceExactlyOnce(t, databasePath, assignment.TaskID, editCall.ID)
	select {
	case message := <-reopened.Messages():
		if message.Assignment != nil {
			t.Fatalf("duplicate Workspace assignment = %+v", *message.Assignment)
		}
		if message.Err != nil {
			t.Fatalf("Workspace worker failed after recovery: %v", message.Err)
		}
	case <-time.After(30 * time.Millisecond):
	}
}

func workspaceOpenCodeBody(t *testing.T, call *completion.ToolCall) []byte {
	t.Helper()
	request := map[string]any{
		"model": "human-expert", "stream": true,
		"messages": []any{map[string]any{
			"role": "user", "content": "apply the reviewed Human workspace edit",
		}},
		"tools": []any{map[string]any{
			"type": "function", "function": map[string]any{
				"name": "edit", "parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"filePath":  map[string]any{"type": "string"},
						"oldString": map[string]any{"type": "string"},
						"newString": map[string]any{"type": "string"},
					},
					"required": []string{"filePath", "oldString", "newString"},
				},
			},
		}},
	}
	if call != nil {
		arguments, err := json.Marshal(call.Input)
		if err != nil {
			t.Fatal(err)
		}
		request["messages"] = append(request["messages"].([]any),
			map[string]any{
				"role": "assistant", "content": nil,
				"tool_calls": []any{map[string]any{
					"id": call.ID, "type": "function", "function": map[string]any{
						"name": call.Name, "arguments": string(arguments),
					},
				}},
			},
			map[string]any{
				"role": "tool", "tool_call_id": call.ID,
				"content": "Edit applied successfully.",
			},
		)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func postWorkspaceCompletion(
	client *http.Client,
	baseURL string,
	body []byte,
	idempotencyKey string,
	callerRoot string,
) callerAttempt {
	request, err := http.NewRequest(
		http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body),
	)
	if err != nil {
		return callerAttempt{err: err}
	}
	request.Header.Set("Authorization", "Bearer "+chaosCallerToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "opencode/"+adapter.OpenCodeVersion+" integration-test")
	request.Header.Set("X-Human-Capability-Tier", string(completion.TierWorkspace))
	request.Header.Set("X-Human-Workspace-Key", workspaceChaosKey)
	request.Header.Set("X-Human-Harness-Id", adapter.OpenCodeID)
	request.Header.Set("X-Human-Harness-Version", adapter.OpenCodeVersion)
	request.Header.Set("X-Human-Workspace-Root", callerRoot)
	request.Header.Set("X-Session-Id", workspaceChaosSessionID)
	request.Header.Set("X-Session-Affinity", workspaceChaosSessionID)
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response, err := client.Do(request)
	if err != nil {
		return callerAttempt{err: err}
	}
	payload, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr == nil {
		readErr = closeErr
	}
	return callerAttempt{status: response.StatusCode, body: payload, err: readErr}
}

func assertOnlyV2Remains(t *testing.T, workspace *mirror.Workspace, v1, v2 []byte) {
	t.Helper()
	pending, err := workspace.Review()
	if err != nil || len(pending) != 1 || pending[0].Kind != mirror.ChangeEdit ||
		!bytes.Equal(pending[0].OldContent, v1) || !bytes.Equal(pending[0].NewContent, v2) {
		t.Fatalf("post-recovery Human review = %+v, %v", pending, err)
	}
}

func assertWorkspaceExactlyOnce(t *testing.T, databasePath, taskID, toolCallID string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		t.Fatal(err)
	}
	var requestCount, completedCount int
	if err := database.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(response_complete), 0)
		FROM completion_requests
		WHERE caller_id = ? AND task_id = ?`, chaosCallerID, taskID,
	).Scan(&requestCount, &completedCount); err != nil {
		t.Fatal(err)
	}
	if requestCount != 2 || completedCount != 2 {
		t.Fatalf("Workspace durable requests/completions = %d/%d, want 2/2", requestCount, completedCount)
	}
	var receiptCount int
	if err := database.QueryRow(`
		SELECT COUNT(*) FROM completion_worker_event_receipts
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ?`,
		chaosCallerID, workspaceChaosFirstKey, workspaceChaosEditEventID,
	).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if receiptCount != 1 {
		t.Fatalf("native edit receipts = %d, want exactly 1", receiptCount)
	}
	var stageCount, distinctKinds int
	if err := database.QueryRow(`
		SELECT COUNT(*), COUNT(DISTINCT kind)
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ?`,
		chaosCallerID, workspaceChaosFirstKey, workspaceChaosEditEventID,
	).Scan(&stageCount, &distinctKinds); err != nil {
		t.Fatal(err)
	}
	if stageCount != 2 || distinctKinds != 2 {
		t.Fatalf("native edit durable stages = %d/%d, want step+applied once", stageCount, distinctKinds)
	}
	var persistedCallID string
	if err := database.QueryRow(`
		SELECT json_extract(data, '$.event.tool_calls[0].id')
		FROM completion_response_events
		WHERE caller_id = ? AND idempotency_key = ? AND event_id = ? AND kind = 'step'`,
		chaosCallerID, workspaceChaosFirstKey, workspaceChaosEditEventID,
	).Scan(&persistedCallID); err != nil {
		t.Fatal(err)
	}
	if persistedCallID != toolCallID {
		t.Fatalf("persisted native tool call id = %q, want %q", persistedCallID, toolCallID)
	}
}
