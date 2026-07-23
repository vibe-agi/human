package anthropic

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func (Codec) NewAggregate(responseID, model string, _ ...dialect.StreamSeed) dialect.Aggregate {
	return &aggregate{responseID: responseID, model: model}
}

type aggregate struct {
	responseID string
	model      string
	started    bool
	done       bool
	text       strings.Builder
}

func (aggregate *aggregate) Start() ([][]byte, error) {
	if aggregate.started {
		return nil, errors.New("Anthropic aggregate already started")
	}
	aggregate.started = true
	return nil, nil
}

func (*aggregate) Heartbeat() []byte { return nil }

func (aggregate *aggregate) Encode(event completion.Event, _ ...dialect.EventSeed) ([][]byte, bool, error) {
	if !aggregate.started {
		return nil, false, errors.New("Anthropic aggregate has not started")
	}
	if aggregate.done {
		return nil, true, errors.New("Anthropic aggregate is complete")
	}
	switch event.Type {
	case completion.EventAccepted:
		return nil, false, nil
	case completion.EventProgress:
		aggregate.text.WriteString(event.Text)
		return nil, false, nil
	case completion.EventFinal, completion.EventClarification:
		aggregate.text.WriteString(event.Text)
		body, err := aggregate.message([]any{map[string]any{
			"type": "text", "text": aggregate.text.String(),
		}}, "end_turn")
		if err != nil {
			return nil, false, err
		}
		aggregate.done = true
		return [][]byte{body}, true, nil
	case completion.EventToolCalls:
		content := make([]any, 0, len(event.ToolCalls)+1)
		if aggregate.text.Len() != 0 {
			content = append(content, map[string]any{"type": "text", "text": aggregate.text.String()})
		}
		for _, call := range event.ToolCalls {
			if call.TextInput != nil {
				return nil, false, fmt.Errorf("Anthropic tool call %q cannot use text input", call.ID)
			}
			input := call.Input
			if input == nil {
				input = map[string]any{}
			}
			content = append(content, map[string]any{
				"type": "tool_use", "id": call.ID, "name": call.Name, "input": input,
			})
		}
		body, err := aggregate.message(content, "tool_use")
		if err != nil {
			return nil, false, err
		}
		aggregate.done = true
		return [][]byte{body}, true, nil
	case completion.EventRejected, completion.EventExpired, completion.EventFailed, completion.EventUnavailable:
		message := event.Error
		if message == "" {
			message = event.ErrorCode
		}
		if message == "" {
			message = "human agent request failed"
		}
		errorType := "api_error"
		switch event.Type {
		case completion.EventRejected:
			errorType = "invalid_request_error"
		case completion.EventUnavailable:
			errorType = "overloaded_error"
		}
		body, err := json.Marshal(map[string]any{
			"type": "error", "error": map[string]string{"type": errorType, "message": message},
		})
		if err != nil {
			return nil, false, err
		}
		aggregate.done = true
		return [][]byte{body}, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported completion event %q", event.Type)
	}
}

func (aggregate *aggregate) message(content []any, stopReason string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"id": aggregate.responseID, "type": "message", "role": "assistant",
		"content": content, "model": aggregate.model,
		"stop_reason": stopReason, "stop_sequence": nil, "stop_details": nil,
		"container": nil, "usage": anthropicUsage(),
	})
}
