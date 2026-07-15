package canonical

import (
	"encoding/json"
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
