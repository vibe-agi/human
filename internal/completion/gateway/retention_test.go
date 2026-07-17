package gateway

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	storeapi "github.com/vibe-agi/human/internal/store"
)

func TestExpiredReplayPayloadReturnsGoneButDigestConflictRemainsConflict(t *testing.T) {
	fixture := newGatewayFixture(t, true)
	body := chatBody("forget this after the replay grace", false)
	workerDone := make(chan error, 1)
	go func() {
		assignment := <-fixture.worker.Assignments
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: "worker-1",
		}); err != nil {
			workerDone <- err
			return
		}
		workerDone <- fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "sensitive final response",
		})
	}()

	first, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "expired-replay"))
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, first.Body)
	first.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first response status = %d", first.StatusCode)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	canonicalRequest, err := openai.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := canonicalRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// Pin the exact stale shape which used to race the retention pass: the
	// idempotency lookup succeeds, then events are pruned before replay starts.
	staleLookup, err := fixture.db.LookupRequest(context.Background(), storeapi.RequestKey{
		CallerID: "caller-1", IdempotencyKey: "expired-replay",
	}, digest)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := fixture.db.PurgeExpiredCompletionPayloads(context.Background(), time.Now().Add(time.Hour)); err != nil || count != 1 {
		t.Fatalf("purge completed response = %d, %v", count, err)
	}
	fixture.worker.Close()
	fixture.worker = nil

	staleRecorder := httptest.NewRecorder()
	fixture.gateway.replayResponse(
		staleRecorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil), staleLookup,
	)
	staleResult := staleRecorder.Result()
	staleBody, readErr := io.ReadAll(staleResult.Body)
	staleResult.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if staleResult.StatusCode != http.StatusGone || !bytes.Contains(staleBody, []byte("replay_payload_expired")) {
		t.Fatalf("lookup/purge replay race = %d, %q", staleResult.StatusCode, staleBody)
	}

	replay, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "expired-replay"))
	if err != nil {
		t.Fatal(err)
	}
	replayBody, readErr := io.ReadAll(replay.Body)
	replay.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if replay.StatusCode != http.StatusGone || !bytes.Contains(replayBody, []byte("replay_payload_expired")) {
		t.Fatalf("expired replay = %d, %q", replay.StatusCode, replayBody)
	}

	conflict, err := http.DefaultClient.Do(newChatRequest(
		t, fixture, chatBody("different payload", false), "expired-replay",
	))
	if err != nil {
		t.Fatal(err)
	}
	conflictBody, readErr := io.ReadAll(conflict.Body)
	conflict.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if conflict.StatusCode != http.StatusConflict || !bytes.Contains(conflictBody, []byte("idempotency_conflict")) {
		t.Fatalf("tombstone digest conflict = %d, %q", conflict.StatusCode, conflictBody)
	}
}
