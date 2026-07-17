package canonical

import (
	"encoding/json"
	"strings"
	"testing"
)

func validRequest() Request {
	return Request{
		Dialect: DialectOpenAIChat,
		Model:   "human-expert",
		Stream:  true,
		Messages: []Message{{
			Role: RoleUser,
			Blocks: []Block{{
				Type: BlockText,
				Text: "diagnose this failure",
			}},
		}},
		Tools: []Tool{{
			Name:        "read_file",
			InputSchema: json.RawMessage(`{"type":"object"}`),
		}},
	}
}

func TestRequestValidateAndDigest(t *testing.T) {
	t.Parallel()
	request := validRequest()
	first, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("digest is unstable: %q != %q", first, second)
	}
	request.Messages[0].Blocks[0].Text = "different"
	third, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("different canonical request has the same digest")
	}
}

func TestRequestDigestIncludesToolPolicyHostedAndOpaqueProviderState(t *testing.T) {
	t.Parallel()
	base := validRequest()
	base.ToolCallPolicy = ToolCallsSerial
	base.HostedCapabilities = []HostedCapability{{
		Type: "web_search", Configuration: json.RawMessage(`{"type":"web_search","external_web_access":true}`),
	}}
	base.OpaqueInput = []OpaqueInput{{
		Type: "reasoning", SHA256: strings.Repeat("a", 64),
	}}
	first, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}

	changed := base
	changed.OpaqueInput = []OpaqueInput{{
		Type: "reasoning", SHA256: strings.Repeat("b", 64),
	}}
	second, err := changed.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("opaque provider state did not affect canonical digest")
	}

	changed = base
	changed.HostedCapabilities = []HostedCapability{{
		Type: "web_search", Configuration: json.RawMessage(`{"type":"web_search","external_web_access":false}`),
	}}
	third, err := changed.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("hosted capability configuration did not affect canonical digest")
	}

	changed = base
	changed.ToolCallPolicy = ToolCallsParallel
	fourth, err := changed.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first == fourth {
		t.Fatal("tool-call policy did not affect canonical digest")
	}
}

func TestToolIdentityUsesNamespaceAndHasReversibleQualifiedName(t *testing.T) {
	t.Parallel()
	request := validRequest()
	request.Tools = []Tool{
		{Name: "read", InputSchema: json.RawMessage(`{}`)},
		{Namespace: "workspace", Name: "read", InputSchema: json.RawMessage(`{}`)},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("same leaf name in distinct namespaces rejected: %v", err)
	}
	if got := request.Tools[1].QualifiedName(); got != "workspace::read" {
		t.Fatalf("qualified name = %q", got)
	}
	request.Tools = append(request.Tools, request.Tools[1])
	if err := request.Validate(); err == nil {
		t.Fatal("duplicate namespace/name pair accepted")
	}
	for _, identity := range [][2]string{{" bad ", "read"}, {"bad::space", "read"}, {"", " bad "}, {"", "bad::name"}} {
		if err := ValidateToolIdentity(identity[0], identity[1]); err == nil {
			t.Fatalf("ambiguous identity %#v accepted", identity)
		}
	}
}

func TestRequestValidationFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*Request)
	}{
		{name: "missing model", mutate: func(request *Request) { request.Model = "" }},
		{name: "missing messages", mutate: func(request *Request) { request.Messages = nil }},
		{name: "invalid schema", mutate: func(request *Request) { request.Tools[0].InputSchema = json.RawMessage(`{`) }},
		{name: "duplicate tool", mutate: func(request *Request) { request.Tools = append(request.Tools, request.Tools[0]) }},
		{name: "empty text", mutate: func(request *Request) { request.Messages[0].Blocks[0].Text = "" }},
		{name: "invalid tool policy", mutate: func(request *Request) { request.ToolCallPolicy = "sometimes" }},
		{name: "invalid hosted configuration", mutate: func(request *Request) {
			request.HostedCapabilities = []HostedCapability{{Type: "web_search", Configuration: json.RawMessage(`{`)}}
		}},
		{name: "invalid opaque input", mutate: func(request *Request) {
			request.OpaqueInput = []OpaqueInput{{Type: "reasoning", SHA256: "not-a-sha256"}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := validRequest()
			tt.mutate(&request)
			if err := request.Validate(); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}

func TestNewOpaqueID(t *testing.T) {
	t.Parallel()
	first, err := NewOpaqueID("task_")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewOpaqueID("task_")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || len(first) <= len("task_") {
		t.Fatalf("unexpected IDs %q and %q", first, second)
	}
}
