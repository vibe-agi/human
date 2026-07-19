package callerhttp_test

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/callerhttp"
)

// TestTransportForwardsAuthenticatedCallerAttributes proves that host
// authenticator claims reach the core admission boundary verbatim, while the
// authenticated CallerID remains the only identity input.
func TestTransportForwardsAuthenticatedCallerAttributes(t *testing.T) {
	want := map[string]any{"org": "acme", "roles": []any{"reviewer"}}
	identity := completionIdentity("attributes-key", "task-attributes", "")
	var seen map[string]any
	endpoint := &fakeEndpoint{}
	endpoint.admit = func(_ context.Context, request llm.AdmissionRequest) (llm.AdmissionResult, error) {
		if request.CallerID != "caller-a" {
			t.Fatalf("caller id = %q", request.CallerID)
		}
		seen = request.CallerAttributes
		return admission(identity, llm.ResponsePage{
			Mode: llm.ResponseStream, DecisionCommitted: true, Complete: true,
			Decision: llm.ResponseDecision{StatusCode: 200, ContentType: "text/event-stream"},
			Events:   []llm.WireEvent{{Sequence: 1, Data: []byte("done\n")}}, Cursor: 1,
		}), nil
	}

	_, runtime, server := startTransport(t, endpoint, callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(context.Context, *http.Request) (callerhttp.Identity, error) {
			return callerhttp.Identity{
				CallerID:   "caller-a",
				Attributes: map[string]any{"org": "acme", "roles": []any{"reviewer"}},
			}, nil
		}),
		Routes: []callerhttp.Route{{Method: http.MethodPost, Path: "/v1/responses", CodecID: testCodec}},
	})
	defer shutdownTransport(t, runtime, server)

	request := mustRequest(t, http.MethodPost, server.URL+"/v1/responses", `{"stream":true}`)
	setWorkspaceHeaders(request, "attributes-key")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("admission attributes = %#v, want %#v", seen, want)
	}
}
