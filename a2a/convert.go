package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"strings"
	"unicode/utf8"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/vibe-agi/human/agent"
)

// WorkspaceExtensionURI identifies Human's Workspace/CAS metadata on A2A Task
// and Artifact values. It carries identities only; applying an Artifact still
// requires the separate durable caller-side CAS journal.
const WorkspaceExtensionURI = "https://github.com/vibe-agi/human/extensions/workspace/v1"

func fromA2AMessage(message *sdk.Message) (agent.MessageInput, error) {
	if message == nil || message.ID == "" {
		return agent.MessageInput{}, protocolError(sdk.ErrInvalidParams, "messageId is required")
	}
	if message.Role != sdk.MessageRoleUser {
		return agent.MessageInput{}, protocolError(sdk.ErrInvalidParams, "incoming message role must be ROLE_USER")
	}
	if len(message.ReferenceTasks) != 0 {
		return agent.MessageInput{}, protocolError(sdk.ErrUnsupportedOperation, "referenceTaskIds are not supported")
	}
	parts := make([]agent.Part, 0, len(message.Parts))
	for _, part := range message.Parts {
		if part == nil {
			return agent.MessageInput{}, protocolError(sdk.ErrInvalidParams, "message part must not be null")
		}
		if part.Filename != "" || len(part.Metadata) != 0 {
			return agent.MessageInput{}, protocolError(sdk.ErrUnsupportedOperation, "part filename and metadata are not supported")
		}
		var converted agent.Part
		switch content := part.Content.(type) {
		case sdk.Text:
			mediaType, base, err := canonicalPartMediaType(part.MediaType, "text/plain; charset=utf-8")
			if err != nil {
				return agent.MessageInput{}, err
			}
			if !strings.HasPrefix(base, "text/") || !utf8.ValidString(string(content)) {
				return agent.MessageInput{}, protocolError(
					sdk.ErrUnsupportedContentType,
					"text part must be valid UTF-8 and use the text/* media type family",
				)
			}
			converted.MediaType = mediaType
			converted.Data = []byte(content)
		case sdk.Raw:
			mediaType, base, err := canonicalPartMediaType(part.MediaType, "application/octet-stream")
			if err != nil {
				return agent.MessageInput{}, err
			}
			// Agent Parts intentionally do not persist the A2A oneof discriminator.
			// Accept only combinations for which mediaType selects Raw again on the
			// outbound path; otherwise an exact retry could silently change Raw into
			// Text or Data.
			if strings.HasPrefix(base, "text/") || base == "application/json" {
				return agent.MessageInput{}, protocolError(
					sdk.ErrUnsupportedContentType,
					"raw part mediaType must be neither text/* nor application/json",
				)
			}
			converted.MediaType = mediaType
			converted.Data = append([]byte(nil), content...)
		case sdk.Data:
			encoded, err := json.Marshal(content.Value)
			if err != nil {
				return agent.MessageInput{}, protocolError(sdk.ErrInvalidParams, "structured part is not valid JSON")
			}
			mediaType, base, err := canonicalPartMediaType(part.MediaType, "application/json")
			if err != nil {
				return agent.MessageInput{}, err
			}
			if base != "application/json" {
				return agent.MessageInput{}, protocolError(
					sdk.ErrUnsupportedContentType,
					"data part mediaType must be application/json",
				)
			}
			converted.MediaType = mediaType
			converted.Data = encoded
		case sdk.URL:
			return agent.MessageInput{}, protocolError(sdk.ErrUnsupportedOperation, "URL parts are not supported")
		default:
			return agent.MessageInput{}, protocolError(sdk.ErrUnsupportedContentType, "message part content type is not supported")
		}
		parts = append(parts, converted)
	}
	return agent.MessageInput{ID: agent.MessageID(message.ID), Parts: parts}, nil
}

func canonicalPartMediaType(value, fallback string) (string, string, error) {
	if value == "" {
		value = fallback
	}
	if value != strings.TrimSpace(value) {
		return "", "", protocolError(sdk.ErrInvalidParams, "part mediaType is not canonical")
	}
	base, _, err := mime.ParseMediaType(value)
	if err != nil {
		return "", "", protocolError(sdk.ErrInvalidParams, "part mediaType is invalid")
	}
	return value, strings.ToLower(base), nil
}

func (handler *requestHandler) convertTask(
	ctx context.Context,
	task agent.Task,
	historyLength *int,
	includeArtifact bool,
) (*sdk.Task, error) {
	if err := validateHistoryLength(historyLength); err != nil {
		return nil, err
	}
	state, err := toA2AState(task.State)
	if err != nil {
		return nil, err
	}
	updated := task.UpdatedAt
	converted := &sdk.Task{
		ID: sdk.TaskID(task.Ref.ID), ContextID: string(task.Context.ID),
		Status: sdk.TaskStatus{State: state, Timestamp: &updated},
	}
	workspaceExtension := workspaceExtensionActive(ctx)
	if workspaceExtension {
		converted.Metadata = map[string]any{
			WorkspaceExtensionURI: map[string]any{"workspaceId": string(task.Ref.Workspace.ID)},
		}
	}
	if (task.State == agent.TaskInputRequired || task.State == agent.TaskCompleted) && task.MessageCount%2 == 0 {
		page, err := handler.config.Agent.ListMessages(ctx, task.Ref, agent.PageRequest{
			After: task.MessageCount - 1, Limit: 1,
		})
		if err != nil {
			return nil, mapAgentError(err)
		}
		if len(page.Items) != 1 || page.Items[0].Author != agent.AuthorAgent {
			return nil, protocolError(sdk.ErrInternalError, "task status message history is corrupt")
		}
		if err := validatePartModes(handler.config.Card.DefaultOutputModes, page.Items[0].Parts, "output"); err != nil {
			return nil, err
		}
		converted.Status.Message = toA2AMessage(page.Items[0], task.Context.ID)
	}
	if historyLength != nil && *historyLength > 0 {
		count := uint64(*historyLength)
		if count > task.MessageCount {
			count = task.MessageCount
		}
		page, err := handler.config.Agent.ListMessages(ctx, task.Ref, agent.PageRequest{
			After: task.MessageCount - count, Limit: int(count),
		})
		if err != nil {
			return nil, mapAgentError(err)
		}
		converted.History = make([]*sdk.Message, 0, len(page.Items))
		for _, message := range page.Items {
			declared := handler.config.Card.DefaultInputModes
			direction := "input"
			if message.Author == agent.AuthorAgent {
				declared = handler.config.Card.DefaultOutputModes
				direction = "output"
			}
			if err := validatePartModes(declared, message.Parts, direction); err != nil {
				return nil, err
			}
			converted.History = append(converted.History, toA2AMessage(message, task.Context.ID))
		}
	}
	if includeArtifact && task.Artifact != nil && task.State == agent.TaskCompleted {
		artifact, err := handler.config.Agent.GetArtifact(ctx, *task.Artifact)
		if err != nil {
			return nil, mapAgentError(err)
		}
		if err := validatePartModes(
			handler.config.Card.DefaultOutputModes,
			[]agent.Part{{MediaType: artifact.Payload.MediaType, Data: artifact.Payload.Data}},
			"output",
		); err != nil {
			return nil, err
		}
		converted.Artifacts = []*sdk.Artifact{toA2AArtifact(artifact, workspaceExtension)}
	}
	return converted, nil
}

func toA2AMessage(message agent.Message, contextID agent.ContextID) *sdk.Message {
	role := sdk.MessageRoleUser
	if message.Author == agent.AuthorAgent {
		role = sdk.MessageRoleAgent
	}
	parts := make(sdk.ContentParts, 0, len(message.Parts))
	for _, part := range message.Parts {
		parts = append(parts, toA2APart(part.MediaType, part.Data))
	}
	return &sdk.Message{
		ID: string(message.ID), TaskID: sdk.TaskID(message.Task.ID), ContextID: string(contextID),
		Role: role, Parts: parts,
	}
}

func toA2APart(mediaType string, data []byte) *sdk.Part {
	base := strings.ToLower(strings.TrimSpace(strings.SplitN(mediaType, ";", 2)[0]))
	var part *sdk.Part
	switch {
	case strings.HasPrefix(base, "text/") && utf8.Valid(data):
		part = sdk.NewTextPart(string(data))
	case base == "application/json":
		var value any
		if json.Unmarshal(data, &value) == nil {
			part = sdk.NewDataPart(value)
		} else {
			part = sdk.NewRawPart(append([]byte(nil), data...))
		}
	default:
		part = sdk.NewRawPart(append([]byte(nil), data...))
	}
	part.MediaType = mediaType
	return part
}

func toA2AArtifact(content agent.ArtifactContent, workspaceExtension bool) *sdk.Artifact {
	artifact := content.Artifact
	// Artifact payload digests cover the exact bytes, not a semantic JSON or
	// text value. Keep every payload in Raw so an A2A marshal/unmarshal cycle
	// cannot normalize whitespace, key order, Unicode, or line endings.
	part := sdk.NewRawPart(append([]byte(nil), content.Payload.Data...))
	part.MediaType = content.Payload.MediaType
	converted := &sdk.Artifact{
		ID:    sdk.ArtifactID(artifact.Ref.ID),
		Parts: sdk.ContentParts{part},
	}
	if workspaceExtension {
		converted.Extensions = []string{WorkspaceExtensionURI}
		converted.Metadata = map[string]any{
			WorkspaceExtensionURI: map[string]any{
				"workspaceId":    string(artifact.Ref.Workspace.ID),
				"artifactDigest": string(artifact.Digest),
				"payloadDigest":  string(artifact.PayloadDigest),
				"baseRevision":   string(artifact.BaseRevision),
				"resultRevision": string(artifact.ResultRevision),
			},
		}
	}
	return converted
}

func workspaceExtensionActive(ctx context.Context) bool {
	extensions, ok := a2asrv.ExtensionsFrom(ctx)
	if !ok {
		return false
	}
	extension := sdk.AgentExtension{URI: WorkspaceExtensionURI}
	return extensions.Active(&extension)
}

func toA2AState(state agent.TaskState) (sdk.TaskState, error) {
	switch state {
	case agent.TaskSubmitted:
		return sdk.TaskStateSubmitted, nil
	case agent.TaskWorking:
		return sdk.TaskStateWorking, nil
	case agent.TaskInputRequired:
		return sdk.TaskStateInputRequired, nil
	case agent.TaskCompleted:
		return sdk.TaskStateCompleted, nil
	case agent.TaskCanceled:
		return sdk.TaskStateCanceled, nil
	case agent.TaskRejected:
		return sdk.TaskStateRejected, nil
	case agent.TaskFailed:
		return sdk.TaskStateFailed, nil
	default:
		return "", protocolError(sdk.ErrInternalError, fmt.Sprintf("unknown Agent task state %q", state))
	}
}

func fromA2AState(state sdk.TaskState) (agent.TaskState, error) {
	switch state {
	case sdk.TaskStateUnspecified:
		return "", nil
	case sdk.TaskStateSubmitted:
		return agent.TaskSubmitted, nil
	case sdk.TaskStateWorking:
		return agent.TaskWorking, nil
	case sdk.TaskStateInputRequired:
		return agent.TaskInputRequired, nil
	case sdk.TaskStateCompleted:
		return agent.TaskCompleted, nil
	case sdk.TaskStateCanceled:
		return agent.TaskCanceled, nil
	case sdk.TaskStateRejected:
		return agent.TaskRejected, nil
	case sdk.TaskStateFailed:
		return agent.TaskFailed, nil
	default:
		return "", protocolError(sdk.ErrInvalidParams, fmt.Sprintf("unsupported task status %q", state))
	}
}

func sameParts(left, right []agent.Part) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].MediaType != right[index].MediaType || !bytes.Equal(left[index].Data, right[index].Data) {
			return false
		}
	}
	return true
}
