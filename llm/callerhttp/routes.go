package callerhttp

import "github.com/vibe-agi/human/llm"

// BuiltinRoutes returns a fresh, explicit route table for Human's built-in
// model API codecs. Hosts may mount, remove, rename, or extend these routes
// before constructing Transport; no request heuristic or implicit fallback is
// enabled by this helper.
func BuiltinRoutes() []Route {
	return []Route{
		{Method: "POST", Path: "/v1/chat/completions", CodecID: llm.CodecID("openai.chat")},
		{Method: "POST", Path: "/v1/responses", CodecID: llm.CodecID("openai.responses")},
		{Method: "POST", Path: "/v1/messages", CodecID: llm.CodecID("anthropic.messages")},
	}
}
