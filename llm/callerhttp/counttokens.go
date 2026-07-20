package callerhttp

import (
	"encoding/json"
	"net/http"
)

// CountTokensPath is Anthropic's token-count endpoint. Claude Code probes it
// on startup and for background-agent resume; a 404 here surfaces to the user
// as "model may not exist", so any deployment serving /v1/messages should
// also mount this handler.
const CountTokensPath = "/v1/messages/count_tokens"

// CountTokensHandler returns a handler for CountTokensPath. A human backend
// has no tokenizer, and the probe only needs a plausible number, so it
// estimates roughly four characters per token over the request's system,
// message, and tool content. Authentication is the mounting host's concern,
// exactly as for every other caller route.
func CountTokensHandler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writeAdapterError(response, http.StatusMethodNotAllowed, "count_tokens requires POST")
			return
		}
		var body struct {
			System   json.RawMessage   `json:"system"`
			Messages []json.RawMessage `json:"messages"`
			Tools    []json.RawMessage `json:"tools"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, defaultBodyLimit))
		if err := decoder.Decode(&body); err != nil {
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(response).Encode(map[string]any{
				"type": "error",
				"error": map[string]string{
					"type": "invalid_request_error", "message": "request body is not valid JSON",
				},
			})
			return
		}
		characters := len(body.System)
		for _, message := range body.Messages {
			characters += len(message)
		}
		for _, tool := range body.Tools {
			characters += len(tool)
		}
		tokens := characters / 4
		if tokens < 1 {
			tokens = 1
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(response).Encode(map[string]int{"input_tokens": tokens})
	})
}
