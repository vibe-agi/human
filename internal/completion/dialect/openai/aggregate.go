package openai

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

// NewAggregate creates the non-streaming Chat Completions encoder. It consumes
// the same canonical worker events as stream, but exposes no intermediate
// bytes: the one JSON response body is produced only by a terminal event.
func (codec Codec) NewAggregate(responseID, model string, seeds ...dialect.StreamSeed) dialect.Aggregate {
	created := codec.now().Unix()
	if len(seeds) != 0 && seeds[0].CreatedAtUnix > 0 {
		created = seeds[0].CreatedAtUnix
	}
	return &aggregate{responseID: responseID, model: model, created: created}
}

type aggregate struct {
	responseID string
	model      string
	created    int64
	started    bool
	done       bool
	text       strings.Builder
}

func (aggregate *aggregate) Start() ([][]byte, error) {
	if aggregate.started {
		return nil, errors.New("OpenAI aggregate already started")
	}
	aggregate.started = true
	return nil, nil
}

func (*aggregate) Heartbeat() []byte { return nil }

func (aggregate *aggregate) Encode(event completion.Event, _ ...dialect.EventSeed) ([][]byte, bool, error) {
	if !aggregate.started {
		return nil, false, errors.New("OpenAI aggregate has not started")
	}
	if aggregate.done {
		return nil, true, errors.New("OpenAI aggregate is complete")
	}
	switch event.Type {
	case completion.EventAccepted:
		return nil, false, nil
	case completion.EventProgress:
		aggregate.text.WriteString(event.Text)
		return nil, false, nil
	case completion.EventFinal, completion.EventClarification:
		aggregate.text.WriteString(event.Text)
		body, err := aggregate.completion(nil, "stop")
		if err != nil {
			return nil, false, err
		}
		aggregate.done = true
		return [][]byte{body}, true, nil
	case completion.EventToolCalls:
		calls := make([]map[string]any, 0, len(event.ToolCalls))
		for _, call := range event.ToolCalls {
			arguments, err := marshalToolArguments(call.Input)
			if err != nil {
				return nil, false, fmt.Errorf("marshal tool call %q arguments: %w", call.ID, err)
			}
			calls = append(calls, map[string]any{
				"id": call.ID, "type": "function",
				"function": map[string]any{"name": call.Name, "arguments": string(arguments)},
			})
		}
		body, err := aggregate.completion(calls, "tool_calls")
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
		body, err := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": message, "type": "human_agent_error",
				"code": nullableAggregateString(event.ErrorCode), "param": nil,
			},
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

func (aggregate *aggregate) completion(toolCalls []map[string]any, finishReason string) ([]byte, error) {
	content := any(aggregate.text.String())
	message := map[string]any{"role": "assistant", "content": content}
	if len(toolCalls) != 0 {
		message["tool_calls"] = toolCalls
		if aggregate.text.Len() == 0 {
			message["content"] = nil
		}
	}
	return json.Marshal(map[string]any{
		"id": aggregate.responseID, "object": "chat.completion",
		"created": aggregate.created, "model": aggregate.model,
		"choices": []map[string]any{{
			"index": 0, "message": message, "finish_reason": finishReason,
		}},
	})
}

func nullableAggregateString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
