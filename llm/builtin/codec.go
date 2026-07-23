// Package builtin exposes HumanLLM's built-in model-API codecs through the
// public llm.Codec port.
//
// These adapters deliberately keep the existing wire implementations behind
// a full value conversion. Public callers never receive an alias of an
// internal canonical value, and the internal encoders never receive an alias
// of a public event.
package builtin

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
	internalanthropic "github.com/vibe-agi/human/internal/completion/dialect/anthropic"
	internalopenai "github.com/vibe-agi/human/internal/completion/dialect/openai"
	internalresponses "github.com/vibe-agi/human/internal/completion/dialect/responses"
	"github.com/vibe-agi/human/llm"
)

const (
	openAIChatVersion = "1.1.0"
	anthropicVersion  = "1.1.0"
	responsesVersion  = "1.3.0"

	// These manifests are the compatibility pins for the current built-in wire
	// projections. Any byte-affecting parser or encoder change must bump both
	// the version and the corresponding manifest before release.
	openAIChatManifest = "human.llm.builtin/openai.chat@1.1.0\nwire=openai-chat-completions-v1\nprojection=2026-07-22\nsdk=openai-go/v3@v3.37.0\ncontrols=explicit-noop-or-reject\n"
	anthropicManifest  = "human.llm.builtin/anthropic.messages@1.1.0\nwire=anthropic-messages-v1\nprojection=2026-07-22\nsdk=anthropic-sdk-go@v1.58.1\ncontrols=explicit-noop-or-reject\n"
	responsesManifest  = "human.llm.builtin/openai.responses@1.3.0\nwire=openai-responses-v1\nprojection=2026-07-23\nsdk=openai-go/v3@v3.37.0\ninclude=reasoning.encrypted_content\ncustom_tools=text,grammar\ncontrols=explicit-noop-or-reject\n"
)

var builtinLimits = llm.CodecLimits{
	MaxRequestBytes:        8 << 20,
	MaxStreamFrameBytes:    16 << 20,
	MaxStreamFramesPerStep: 4096,
	MaxAggregateBytes:      64 << 20,
	MaxAdmissionErrorBytes: 1 << 20,
}

// Registrations returns fresh registrations for every built-in model API.
// Applications may remove, replace, or append codecs before constructing a
// Service; mutating the returned slice never changes a later call.
func Registrations() []llm.CodecRegistration {
	return []llm.CodecRegistration{
		{
			Codec: OpenAIChat(), StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		},
		{
			Codec: OpenAIResponses(), StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		},
		{
			Codec: AnthropicMessages(), StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		},
	}
}

// OpenAIChat returns the built-in OpenAI Chat Completions codec.
func OpenAIChat() llm.Codec {
	return newCodec("openai.chat", openAIChatVersion, openAIChatManifest, internalopenai.New())
}

// AnthropicMessages returns the built-in Anthropic Messages codec.
func AnthropicMessages() llm.Codec {
	return newCodec("anthropic.messages", anthropicVersion, anthropicManifest, internalanthropic.New())
}

// OpenAIResponses returns the built-in OpenAI Responses codec.
func OpenAIResponses() llm.Codec {
	return newCodec("openai.responses", responsesVersion, responsesManifest, internalresponses.New())
}

type codec struct {
	inner       dialect.Codec
	description llm.CodecDescription
}

var _ llm.Codec = (*codec)(nil)

func newCodec(id llm.CodecID, version, manifest string, inner dialect.Codec) *codec {
	return &codec{
		inner: inner,
		description: llm.CodecDescription{
			Contract: framework.Contract{
				ID:    llm.CodecContractID,
				Major: llm.CodecContractMajor,
			},
			ID:               id,
			Version:          version,
			Fingerprint:      llm.Fingerprint([]byte(manifest)),
			Limits:           builtinLimits,
			OverloadedStatus: inner.OverloadedStatus(),
		},
	}
}

func (codec *codec) Description() llm.CodecDescription {
	return codec.description
}

func (codec *codec) Decode(body []byte) (llm.Request, error) {
	if err := codec.description.Limits.CheckRequestSize(int64(len(body))); err != nil {
		return llm.Request{}, err
	}
	decoded, err := codec.inner.Decode(body)
	if err != nil {
		return llm.Request{}, err
	}
	request, err := requestFromInternal(decoded)
	if err != nil {
		return llm.Request{}, fmt.Errorf("builtin codec %q: convert decoded request: %w", codec.description.ID, err)
	}
	if err := request.Validate(); err != nil {
		return llm.Request{}, fmt.Errorf("builtin codec %q: validate decoded request: %w", codec.description.ID, err)
	}
	return request, nil
}

func (codec *codec) NewStream(session llm.EncoderSession) (llm.Encoder, error) {
	seed, err := internalSessionSeed(session)
	if err != nil {
		return nil, err
	}
	return &encoder{inner: codec.inner.NewStream(session.ResponseID, session.Model, seed)}, nil
}

func (codec *codec) NewAggregate(session llm.EncoderSession) (llm.Encoder, error) {
	seed, err := internalSessionSeed(session)
	if err != nil {
		return nil, err
	}
	return &encoder{inner: codec.inner.NewAggregate(session.ResponseID, session.Model, seed)}, nil
}

func (codec *codec) AdmissionError(failure llm.AdmissionFailure) ([]byte, error) {
	if err := failure.Validate(); err != nil {
		return nil, err
	}
	return bytes.Clone(codec.inner.AdmissionError(failure.Status, failure.Code, failure.Message)), nil
}

func internalSessionSeed(session llm.EncoderSession) (dialect.StreamSeed, error) {
	if err := session.Validate(); err != nil {
		return dialect.StreamSeed{}, err
	}
	if len(session.Seed.Entropy) != 0 || len(session.Seed.Opaque) != 0 {
		return dialect.StreamSeed{}, fmt.Errorf(
			"%w: built-in codecs do not accept session entropy or opaque seeds",
			llm.ErrInvalidCodecContract,
		)
	}
	policy, err := toolCallPolicyToInternal(session.Seed.ToolCallPolicy)
	if err != nil {
		return dialect.StreamSeed{}, err
	}
	return dialect.StreamSeed{
		CreatedAtUnix:  session.Seed.CreatedAtUnix,
		ToolCallPolicy: policy,
	}, nil
}

type encoder struct {
	inner dialect.Encoder
}

var _ llm.Encoder = (*encoder)(nil)

func (encoder *encoder) Start() ([][]byte, error) {
	frames, err := encoder.inner.Start()
	return cloneFrames(frames), err
}

func (encoder *encoder) Encode(event llm.Event, seed llm.EventSeed) ([][]byte, bool, error) {
	if err := seed.Validate(); err != nil {
		return nil, false, err
	}
	if seed.EncodedAtUnix <= 0 {
		return nil, false, fmt.Errorf(
			"%w: positive encoded-at seed is required",
			llm.ErrInvalidCodecContract,
		)
	}
	if len(seed.Entropy) != 0 || len(seed.Opaque) != 0 {
		return nil, false, fmt.Errorf(
			"%w: built-in codecs do not accept event entropy or opaque seeds",
			llm.ErrInvalidCodecContract,
		)
	}
	converted, err := eventToInternal(event)
	if err != nil {
		return nil, false, err
	}
	frames, done, err := encoder.inner.Encode(converted, dialect.EventSeed{
		EncodedAtUnix: seed.EncodedAtUnix,
	})
	return cloneFrames(frames), done, err
}

func requestFromInternal(request canonical.Request) (llm.Request, error) {
	policy, err := toolCallPolicyFromInternal(request.ToolCallPolicy)
	if err != nil {
		return llm.Request{}, err
	}
	converted := llm.Request{
		Model:          request.Model,
		Stream:         request.Stream,
		System:         request.System,
		ToolCallPolicy: policy,
	}
	if request.Messages != nil {
		converted.Messages = make([]llm.Message, len(request.Messages))
		for messageIndex, message := range request.Messages {
			role, err := roleFromInternal(message.Role)
			if err != nil {
				return llm.Request{}, err
			}
			converted.Messages[messageIndex].Role = role
			if message.Blocks != nil {
				converted.Messages[messageIndex].Blocks = make([]llm.Block, len(message.Blocks))
				for blockIndex, block := range message.Blocks {
					convertedBlock, err := blockFromInternal(block)
					if err != nil {
						return llm.Request{}, err
					}
					converted.Messages[messageIndex].Blocks[blockIndex] = convertedBlock
				}
			}
		}
	}
	if request.Tools != nil {
		converted.Tools = make([]llm.Tool, len(request.Tools))
		for index, tool := range request.Tools {
			converted.Tools[index] = llm.Tool{
				Namespace:   tool.Namespace,
				Name:        tool.Name,
				Description: tool.Description,
				InputKind:   llm.ToolInputKind(tool.InputKind),
				InputSchema: bytes.Clone(tool.InputSchema),
				InputFormat: bytes.Clone(tool.InputFormat),
			}
		}
	}
	if request.HostedCapabilities != nil {
		converted.HostedCapabilities = make([]llm.HostedCapability, len(request.HostedCapabilities))
		for index, capability := range request.HostedCapabilities {
			converted.HostedCapabilities[index] = llm.HostedCapability{
				Type:          capability.Type,
				Configuration: bytes.Clone(capability.Configuration),
			}
		}
	}
	if request.OpaqueInput != nil {
		converted.OpaqueInput = make([]llm.OpaqueInput, len(request.OpaqueInput))
		for index, input := range request.OpaqueInput {
			converted.OpaqueInput[index] = llm.OpaqueInput{
				Type:   input.Type,
				SHA256: input.SHA256,
			}
		}
	}
	if request.Metadata != nil {
		converted.Metadata = make(map[string]string, len(request.Metadata))
		for key, value := range request.Metadata {
			converted.Metadata[key] = value
		}
	}
	return converted, nil
}

func blockFromInternal(block canonical.Block) (llm.Block, error) {
	blockType, err := blockTypeFromInternal(block.Type)
	if err != nil {
		return llm.Block{}, err
	}
	input, err := cloneJSONObject(block.Input)
	if err != nil {
		return llm.Block{}, fmt.Errorf("clone tool input: %w", err)
	}
	output, err := outputFromInternal(block.Output)
	if err != nil {
		return llm.Block{}, fmt.Errorf("clone tool output: %w", err)
	}
	return llm.Block{
		Type:          blockType,
		Text:          block.Text,
		ImageURL:      block.ImageURL,
		ToolCallID:    block.ToolCallID,
		ToolNamespace: block.ToolNamespace,
		ToolName:      block.ToolName,
		Input:         input,
		TextInput:     cloneString(block.TextInput),
		Output:        output,
		IsError:       block.IsError,
	}, nil
}

func outputFromInternal(value any) (any, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case canonical.Block:
		return blockFromInternal(value)
	case []canonical.Block:
		blocks := make([]llm.Block, len(value))
		for index, block := range value {
			converted, err := blockFromInternal(block)
			if err != nil {
				return nil, err
			}
			blocks[index] = converted
		}
		return blocks, nil
	default:
		return cloneJSONValue(value)
	}
}

func eventToInternal(event llm.Event) (completion.Event, error) {
	eventType, err := eventTypeToInternal(event.Type)
	if err != nil {
		return completion.Event{}, err
	}
	converted := completion.Event{
		ID:        event.ID,
		Type:      eventType,
		WorkerID:  event.WorkerID,
		Text:      event.Text,
		ErrorCode: event.ErrorCode,
		Error:     event.Error,
	}
	if event.ToolCalls != nil {
		converted.ToolCalls = make([]completion.ToolCall, len(event.ToolCalls))
		for index, call := range event.ToolCalls {
			if call.ID == "" {
				return completion.Event{}, fmt.Errorf("%w: tool call %d has no id", llm.ErrInvalidCodecContract, index)
			}
			if err := llm.ValidateToolIdentity(call.Namespace, call.Name); err != nil {
				return completion.Event{}, fmt.Errorf(
					"%w: tool call %d: %v", llm.ErrInvalidCodecContract, index, err,
				)
			}
			input, err := cloneJSONObject(call.Input)
			if err != nil {
				return completion.Event{}, fmt.Errorf("clone tool call %d input: %w", index, err)
			}
			converted.ToolCalls[index] = completion.ToolCall{
				ID: call.ID, Namespace: call.Namespace, Name: call.Name, Input: input,
				TextInput: cloneString(call.TextInput),
			}
		}
	}
	return converted, nil
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneJSONObject(value map[string]any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	cloned, err := cloneJSONValue(value)
	if err != nil {
		return nil, err
	}
	object, ok := cloned.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JSON value is %T, want object", cloned)
	}
	return object, nil
}

func cloneJSONValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var cloned any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

func cloneFrames(frames [][]byte) [][]byte {
	if frames == nil {
		return nil
	}
	cloned := make([][]byte, len(frames))
	for index, frame := range frames {
		cloned[index] = bytes.Clone(frame)
	}
	return cloned
}

func roleFromInternal(role canonical.Role) (llm.Role, error) {
	switch role {
	case canonical.RoleSystem:
		return llm.RoleSystem, nil
	case canonical.RoleUser:
		return llm.RoleUser, nil
	case canonical.RoleAssistant:
		return llm.RoleAssistant, nil
	case canonical.RoleTool:
		return llm.RoleTool, nil
	default:
		return "", fmt.Errorf("unsupported internal role %q", role)
	}
}

func blockTypeFromInternal(blockType canonical.BlockType) (llm.BlockType, error) {
	switch blockType {
	case canonical.BlockText:
		return llm.BlockText, nil
	case canonical.BlockImage:
		return llm.BlockImage, nil
	case canonical.BlockToolUse:
		return llm.BlockToolUse, nil
	case canonical.BlockToolResult:
		return llm.BlockToolResult, nil
	default:
		return "", fmt.Errorf("unsupported internal block type %q", blockType)
	}
}

func toolCallPolicyFromInternal(policy canonical.ToolCallPolicy) (llm.ToolCallPolicy, error) {
	switch policy {
	case canonical.ToolCallsDisabled:
		return llm.ToolCallsDisabled, nil
	case canonical.ToolCallsSerial:
		return llm.ToolCallsSerial, nil
	case canonical.ToolCallsParallel:
		return llm.ToolCallsParallel, nil
	case "":
		return "", nil
	default:
		return "", fmt.Errorf("unsupported internal tool-call policy %q", policy)
	}
}

func toolCallPolicyToInternal(policy llm.ToolCallPolicy) (canonical.ToolCallPolicy, error) {
	switch policy {
	case "":
		return "", nil
	case llm.ToolCallsDisabled:
		return canonical.ToolCallsDisabled, nil
	case llm.ToolCallsSerial:
		return canonical.ToolCallsSerial, nil
	case llm.ToolCallsParallel:
		return canonical.ToolCallsParallel, nil
	default:
		return "", fmt.Errorf("%w: unsupported tool-call policy %q", llm.ErrInvalidCodecContract, policy)
	}
}

func eventTypeToInternal(eventType llm.EventType) (completion.EventType, error) {
	switch eventType {
	case llm.EventAccepted:
		return completion.EventAccepted, nil
	case llm.EventProgress:
		return completion.EventProgress, nil
	case llm.EventFinal:
		return completion.EventFinal, nil
	case llm.EventClarification:
		return completion.EventClarification, nil
	case llm.EventToolCalls:
		return completion.EventToolCalls, nil
	case llm.EventRejected:
		return completion.EventRejected, nil
	case llm.EventExpired:
		return completion.EventExpired, nil
	case llm.EventFailed:
		return completion.EventFailed, nil
	case llm.EventUnavailable:
		return completion.EventUnavailable, nil
	default:
		return "", fmt.Errorf("%w: unsupported event type %q", llm.ErrInvalidCodecContract, eventType)
	}
}
