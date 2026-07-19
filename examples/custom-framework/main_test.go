package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
)

func TestCustomFrameworkExample(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := run(ctx, &output); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{
		"audit store.bind",
		"audit store.view",
		"audit store.update",
		"audit protector.seal",
		"audit protector.open",
		"HumanLLM status=200",
		"Hello from a Human worker.",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("output does not contain %q:\n%s", want, text)
		}
	}
}

func TestInProcessTransportRejectsBrokenResponsePages(t *testing.T) {
	identity := llm.CompletionIdentity{
		CallerID: "caller-a", RequestID: "request-a", TaskID: "task-a",
		IdempotencyKey: "key-a",
	}
	digest := llm.StoreDigest("sha256:example")
	tests := []struct {
		name   string
		page   llm.ResponsePage
		after  uint64
		waited bool
	}{
		{
			name: "identity changed",
			page: llm.ResponsePage{
				Identity: llm.CompletionIdentity{
					CallerID: "caller-b", RequestID: "request-a", TaskID: "task-a",
					IdempotencyKey: "key-a",
				},
				RequestDigest: digest, Mode: llm.ResponseAggregate,
			},
		},
		{
			name: "wait made no progress",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest,
				Mode: llm.ResponseAggregate, Cursor: 3,
			},
			after: 3, waited: true,
		},
		{
			name: "event is not after cursor",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest,
				Mode: llm.ResponseStream, Cursor: 3,
				Events: []llm.WireEvent{{Sequence: 2, Data: []byte("stale")}},
			},
			after: 2, waited: true,
		},
		{
			name: "complete without decision",
			page: llm.ResponsePage{
				Identity: identity, RequestDigest: digest,
				Mode: llm.ResponseAggregate, Complete: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePage(
				test.page, identity, digest, test.page.Mode, test.after, test.waited,
			)
			if !errors.Is(err, errEndpointProtocol) {
				t.Fatalf("validatePage error = %v, want errEndpointProtocol", err)
			}
		})
	}
}
