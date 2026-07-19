package callerhttp_test

import (
	"reflect"
	"testing"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/callerhttp"
)

func TestBuiltinRoutesAreExplicitAndFresh(t *testing.T) {
	want := []callerhttp.Route{
		{Method: "POST", Path: "/v1/chat/completions", CodecID: llm.CodecID("openai.chat")},
		{Method: "POST", Path: "/v1/responses", CodecID: llm.CodecID("openai.responses")},
		{Method: "POST", Path: "/v1/messages", CodecID: llm.CodecID("anthropic.messages")},
	}
	first := callerhttp.BuiltinRoutes()
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("BuiltinRoutes() = %#v, want %#v", first, want)
	}
	first[0].Path = "/mutated"
	if second := callerhttp.BuiltinRoutes(); !reflect.DeepEqual(second, want) {
		t.Fatalf("BuiltinRoutes() reused mutable state: %#v", second)
	}
}
