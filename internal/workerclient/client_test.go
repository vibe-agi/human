package workerclient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/workerproto"
)

func TestDialRejectsPermanentlyInvalidEndpointBeforeCreatingOutbox(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{
		"127.0.0.1:19080", "ftp://127.0.0.1/worker", "ws:///worker",
		"ws://gateway.example/worker", "http://192.0.2.10/worker",
		"wss://user:secret@gateway.example/worker", "wss://gateway.example/worker#fragment",
	} {
		endpoint := endpoint
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			outboxPath := filepath.Join(t.TempDir(), "outbox.db")
			client, err := DialWithOutbox(context.Background(), endpoint, "token", outboxPath)
			if client != nil || err == nil {
				if client != nil {
					_ = client.Close()
				}
				t.Fatalf("client=%v error=%v, want permanent endpoint validation error", client, err)
			}
			if _, statErr := os.Stat(outboxPath); !os.IsNotExist(statErr) {
				t.Fatalf("invalid endpoint created outbox %q: %v", outboxPath, statErr)
			}
			if strings.Contains(err.Error(), "connection refused") {
				t.Fatalf("invalid endpoint reached network dial: %v", err)
			}
		})
	}
}

func TestWorkerEndpointAllowsPlaintextOnlyOnLoopback(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{
		"ws://localhost:19080/worker",
		"http://127.0.0.1:19080/worker",
		"ws://[::1]:19080/worker",
		"wss://gateway.example/worker",
		"https://gateway.example/worker",
	} {
		if err := validateWorkerEndpoint(endpoint); err != nil {
			t.Fatalf("safe worker endpoint %q: %v", endpoint, err)
		}
	}
}

func TestDeliverBackpressuresAssignmentWithoutCancelingClient(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{
		cancel:   cancel,
		messages: make(chan Message, 1),
	}
	queuedError := errors.New("already queued")
	client.deliver(ctx, Message{Err: queuedError})

	assignment := completion.Assignment{TaskID: "task-backpressure"}
	started := make(chan struct{})
	delivered := make(chan struct{})
	go func() {
		close(started)
		client.deliver(ctx, Message{Assignment: &assignment})
		close(delivered)
	}()
	<-started

	select {
	case <-delivered:
		t.Fatal("assignment was dropped while the UI message buffer was full")
	case <-time.After(20 * time.Millisecond):
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("full UI message buffer canceled the client: %v", err)
	}
	if message := <-client.messages; !errors.Is(message.Err, queuedError) {
		t.Fatalf("first buffered message = %+v", message)
	}

	select {
	case message := <-client.messages:
		if message.Assignment == nil || message.Assignment.TaskID != assignment.TaskID {
			t.Fatalf("backpressured assignment = %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("assignment did not resume after the UI consumed buffer capacity")
	}
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("assignment delivery remained blocked after buffer capacity was available")
	}
}

func TestDeliverDropsAdvisoryMessageWithoutCancelingClient(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{
		cancel:   cancel,
		messages: make(chan Message, 1),
	}
	first := errors.New("first")
	client.deliver(ctx, Message{Err: first})

	returned := make(chan struct{})
	go func() {
		client.deliver(ctx, Message{Err: errors.New("duplicate status")})
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("advisory delivery blocked on a full UI message buffer")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("advisory overflow canceled the client: %v", err)
	}
	if message := <-client.messages; !errors.Is(message.Err, first) {
		t.Fatalf("buffered advisory message = %+v", message)
	}
	select {
	case extra := <-client.messages:
		t.Fatalf("overflowing advisory message was unexpectedly queued: %+v", extra)
	default:
	}
}

func TestDeliverBackpressuresEventRejection(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{messages: make(chan Message, 1)}
	client.deliver(ctx, Message{Err: errors.New("already queued")})
	rejected := &workerproto.EventRejected{
		CallerID: "caller", IdempotencyKey: "request", EventID: "event",
		Message: "completion session is already closed",
	}

	delivered := make(chan struct{})
	go func() {
		client.deliver(ctx, Message{EventRejected: rejected})
		close(delivered)
	}()
	select {
	case <-delivered:
		t.Fatal("event rejection was dropped while the UI message buffer was full")
	case <-time.After(20 * time.Millisecond):
	}
	<-client.messages
	select {
	case message := <-client.messages:
		if message.EventRejected == nil || message.EventRejected.EventID != rejected.EventID {
			t.Fatalf("backpressured event rejection = %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("event rejection did not resume after buffer capacity was available")
	}
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("event rejection delivery remained blocked after being consumed")
	}
}
