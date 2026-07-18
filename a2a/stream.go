package a2a

import (
	"context"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

func (handler *requestHandler) tailEvents(
	ctx context.Context,
	ref agent.TaskRef,
	cursor uint64,
	stopOnInputRequired bool,
	yield func(sdk.Event, error) bool,
) {
	ticker := time.NewTicker(handler.poll)
	defer ticker.Stop()
	for {
		page, err := handler.config.Agent.ReadEvents(ctx, ref, agent.PageRequest{After: cursor})
		if err != nil {
			yield(nil, mapAgentError(err))
			return
		}
		for _, event := range page.Items {
			cursor = event.Sequence
			converted, terminal, err := handler.convertEvent(ctx, event)
			if err != nil {
				yield(nil, err)
				return
			}
			for _, value := range converted {
				if !yield(value, nil) {
					return
				}
			}
			if terminal || (stopOnInputRequired && event.State == agent.TaskInputRequired) {
				return
			}
		}
		if page.HasMore {
			continue
		}
		select {
		case <-ctx.Done():
			yield(nil, ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

func (handler *requestHandler) convertEvent(ctx context.Context, event agent.Event) ([]sdk.Event, bool, error) {
	if event.Type == agent.EventArtifactFrozen {
		return nil, false, nil
	}
	state, err := toA2AState(event.State)
	if err != nil {
		return nil, false, err
	}
	info := sdk.TaskInfo{TaskID: sdk.TaskID(event.Task.ID)}
	task, err := handler.config.Agent.GetTask(ctx, event.Task)
	if err != nil {
		return nil, false, mapAgentError(err)
	}
	info.ContextID = string(task.Context.ID)
	var statusMessage *sdk.Message
	if event.Message != "" {
		message, err := handler.config.Agent.GetMessage(ctx, event.Task.Workspace.Authority, event.Message)
		if err != nil {
			return nil, false, mapAgentError(err)
		}
		if message.Author == agent.AuthorAgent {
			if err := validatePartModes(handler.config.Card.DefaultOutputModes, message.Parts, "output"); err != nil {
				return nil, false, err
			}
			statusMessage = toA2AMessage(message, task.Context.ID)
		}
	}
	result := make([]sdk.Event, 0, 2)
	if event.Type == agent.EventTaskCompleted && event.Artifact != "" {
		content, err := handler.config.Agent.GetArtifact(ctx, agent.ArtifactRef{
			Workspace: event.Task.Workspace, ID: event.Artifact,
		})
		if err != nil {
			return nil, false, mapAgentError(err)
		}
		if err := validatePartModes(
			handler.config.Card.DefaultOutputModes,
			[]agent.Part{{MediaType: content.Payload.MediaType, Data: content.Payload.Data}},
			"output",
		); err != nil {
			return nil, false, err
		}
		result = append(result, &sdk.TaskArtifactUpdateEvent{
			TaskID: info.TaskID, ContextID: info.ContextID,
			Artifact: toA2AArtifact(content, workspaceExtensionActive(ctx)), LastChunk: true,
		})
	}
	occurred := event.OccurredAt
	result = append(result, &sdk.TaskStatusUpdateEvent{
		TaskID: info.TaskID, ContextID: info.ContextID,
		Status: sdk.TaskStatus{State: state, Message: statusMessage, Timestamp: &occurred},
	})
	return result, event.State.Terminal(), nil
}
