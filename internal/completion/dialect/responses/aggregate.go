package responses

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
)

func (codec Codec) NewAggregate(responseID, model string, seeds ...dialect.StreamSeed) dialect.Aggregate {
	created := codec.now().Unix()
	parallelToolCalls := true
	toolChoice := "auto"
	if len(seeds) != 0 && seeds[0].CreatedAtUnix > 0 {
		created = seeds[0].CreatedAtUnix
	}
	if len(seeds) != 0 && seeds[0].ToolCallPolicy == canonical.ToolCallsSerial {
		parallelToolCalls = false
	}
	if len(seeds) != 0 && seeds[0].ToolCallPolicy == canonical.ToolCallsDisabled {
		parallelToolCalls = false
		toolChoice = "none"
	}
	return &aggregate{
		responseID: responseID, model: model, created: created, now: codec.now,
		parallelToolCalls: parallelToolCalls,
		toolChoice:        toolChoice,
	}
}

type aggregate struct {
	responseID        string
	model             string
	created           int64
	now               func() time.Time
	started           bool
	done              bool
	text              strings.Builder
	parallelToolCalls bool
	toolChoice        string
}

func (aggregate *aggregate) Start() ([][]byte, error) {
	if aggregate.started {
		return nil, errors.New("Responses aggregate already started")
	}
	aggregate.started = true
	return nil, nil
}

func (*aggregate) Heartbeat() []byte { return nil }

func (aggregate *aggregate) Encode(event completion.Event, seeds ...dialect.EventSeed) ([][]byte, bool, error) {
	if !aggregate.started {
		return nil, false, errors.New("Responses aggregate has not started")
	}
	if aggregate.done {
		return nil, true, errors.New("Responses aggregate is complete")
	}
	switch event.Type {
	case completion.EventAccepted:
		return nil, false, nil
	case completion.EventProgress:
		aggregate.text.WriteString(event.Text)
		return nil, false, nil
	case completion.EventFinal, completion.EventClarification:
		aggregate.text.WriteString(event.Text)
		// A successful final always carries an assistant message, including an
		// explicit empty output_text part. Returning output=[] makes clients lose
		// the assistant turn and can make the next round trip invalid.
		output := []any{aggregate.messageOutput()}
		body, err := json.Marshal(aggregate.response("completed", output, encodedAtUnix(aggregate.now, seeds)))
		if err != nil {
			return nil, false, err
		}
		aggregate.done = true
		return [][]byte{body}, true, nil
	case completion.EventToolCalls:
		output := make([]any, 0, len(event.ToolCalls)+1)
		if aggregate.text.Len() != 0 {
			output = append(output, aggregate.messageOutput())
		}
		for _, call := range event.ToolCalls {
			if call.TextInput != nil {
				item := map[string]any{
					"id": customItemID(call.ID), "type": "custom_tool_call", "status": "completed",
					"call_id": call.ID, "name": call.Name, "input": *call.TextInput,
				}
				if call.Namespace != "" {
					item["namespace"] = call.Namespace
				}
				output = append(output, item)
				continue
			}
			arguments, err := marshalToolArguments(call.Input)
			if err != nil {
				return nil, false, fmt.Errorf("marshal tool call %q arguments: %w", call.ID, err)
			}
			item := map[string]any{
				"id": functionItemID(call.ID), "type": "function_call", "status": "completed",
				"call_id": call.ID, "name": call.Name, "arguments": string(arguments),
			}
			if call.Namespace != "" {
				item["namespace"] = call.Namespace
			}
			output = append(output, item)
		}
		body, err := json.Marshal(aggregate.response("completed", output, encodedAtUnix(aggregate.now, seeds)))
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
				"code": nullableString(event.ErrorCode), "param": nil,
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

func (aggregate *aggregate) response(status string, output []any, completedAt int64) map[string]any {
	var usage any
	if status == "completed" {
		usage = responsesUsage()
	}
	return map[string]any{
		"id": aggregate.responseID, "object": "response", "created_at": aggregate.created,
		"status": status, "completed_at": completedAt, "error": nil,
		"incomplete_details": nil, "instructions": nil, "max_output_tokens": nil,
		"model": aggregate.model, "output": output, "parallel_tool_calls": aggregate.parallelToolCalls,
		"previous_response_id": nil,
		"reasoning":            map[string]any{"effort": nil, "summary": nil},
		"store":                false, "temperature": nil,
		"text":        map[string]any{"format": map[string]string{"type": "text"}},
		"tool_choice": aggregate.toolChoice, "tools": []any{}, "top_p": nil,
		"truncation": "disabled", "usage": usage, "metadata": map[string]string{},
	}
}

func (aggregate *aggregate) messageOutput() map[string]any {
	return map[string]any{
		"id": messageItemID(aggregate.responseID), "type": "message", "status": "completed",
		"role": "assistant", "content": []any{outputTextPart(aggregate.text.String())},
	}
}
