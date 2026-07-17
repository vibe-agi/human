package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	storeapi "github.com/vibe-agi/human/internal/store"
	"github.com/vibe-agi/human/internal/workerproto"
)

func TestDefaultCompletionLimitsUseWorkerWireBudget(t *testing.T) {
	config := (Config{}).withDefaults()
	if config.MaxBodyBytes != workerproto.MaxWireMessageBytes ||
		config.MaxWorkerMessageBytes != workerproto.MaxWireMessageBytes {
		t.Fatalf("default limits = body %d, worker %d, want %d",
			config.MaxBodyBytes, config.MaxWorkerMessageBytes, workerproto.MaxWireMessageBytes)
	}
	configured := (Config{MaxBodyBytes: 2 << 20, MaxWorkerMessageBytes: 1 << 20}).withDefaults()
	if configured.MaxBodyBytes != configured.MaxWorkerMessageBytes {
		t.Fatalf("HTTP body limit %d exceeds worker limit %d", configured.MaxBodyBytes, configured.MaxWorkerMessageBytes)
	}
}

func TestCompletionRejectsExpandedAssignmentBeforeDurableAdmission(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		MaxBodyBytes:          4 << 10,
		MaxWorkerMessageBytes: 1 << 10,
	})
	// '<' is one byte on the incoming wire but json.Encoder escapes it as six
	// bytes in the canonical assignment. This exercises the exact envelope
	// gate, not only the coarse HTTP body limit.
	body := chatBody(strings.Repeat("<", 256), false)
	if int64(len(body)) >= fixture.gateway.config.MaxBodyBytes {
		t.Fatalf("test request unexpectedly exceeds coarse body limit: %d", len(body))
	}

	response, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "oversized-assignment"))
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusRequestEntityTooLarge || !strings.Contains(string(data), "request_too_large") {
		t.Fatalf("oversized assignment response = %d, %s", response.StatusCode, data)
	}

	canonicalRequest, err := openai.New().Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := canonicalRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.db.LookupRequest(context.Background(), storeapi.RequestKey{
		CallerID: "caller-1", IdempotencyKey: "oversized-assignment",
	}, digest)
	if !errors.Is(err, storeapi.ErrNotFound) {
		t.Fatalf("oversized assignment reached durable admission: %v", err)
	}
	select {
	case assignment := <-fixture.worker.Assignments:
		t.Fatalf("oversized assignment reached worker: %+v", assignment)
	default:
	}
}

func TestCompletionBodyLimitReturns413(t *testing.T) {
	fixture := newGatewayFixtureWithConfig(t, true, Config{
		MaxBodyBytes:          128,
		MaxWorkerMessageBytes: 1 << 10,
	})
	body := chatBody(strings.Repeat("x", 256), false)
	response, err := http.DefaultClient.Do(newChatRequest(t, fixture, body, "oversized-body"))
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusRequestEntityTooLarge || !strings.Contains(string(data), "request_too_large") {
		t.Fatalf("oversized body response = %d, %s", response.StatusCode, data)
	}
}
