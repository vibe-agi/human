package a2aadapter

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/internal/delegation"
)

const (
	// MetadataKey contains authority-owned task and artifact projection data.
	// Caller-provided values at this key are intentionally replaced.
	MetadataKey = delegation.MetadataKey
	// RequestMetadataKey is reserved for caller request parameters that must
	// survive the authority projection unchanged.
	RequestMetadataKey = delegation.RequestMetadataKey
	opaqueMetadataKey  = MetadataKey + "/opaque"
)

func toA2ATask(
	snapshot delegation.Snapshot,
	historyLength *int,
	includeArtifacts bool,
	includeExec bool,
) (*a2a.Task, error) {
	metadata := decodeMetadata(snapshot.Task.Metadata)
	authorityMetadata := map[string]any{
		"revision":   snapshot.Task.Revision,
		"latestTurn": snapshot.Task.LatestTurn,
		"nextTurn":   snapshot.Task.NextTurn,
	}
	if includeExec {
		authorityMetadata["execRequests"] = snapshot.Exec
	}
	if snapshot.Task.State == delegation.StateRewindPending {
		authorityMetadata["state"] = string(delegation.StateRewindPending)
		if snapshot.Task.PendingRewindTo != nil {
			authorityMetadata["rewindTo"] = *snapshot.Task.PendingRewindTo
		}
	}
	metadata[MetadataKey] = authorityMetadata
	updatedAt := snapshot.Task.UpdatedAt
	task := &a2a.Task{
		ID:        a2a.TaskID(snapshot.Task.ID),
		ContextID: snapshot.Task.ContextID,
		Metadata:  metadata,
		Status: a2a.TaskStatus{
			State:     toA2AState(snapshot.Task.State),
			Timestamp: &updatedAt,
		},
	}

	history := make([]*a2a.Message, 0, len(snapshot.Messages))
	for _, stored := range snapshot.Messages {
		message := new(a2a.Message)
		if err := json.Unmarshal(stored.Data, message); err != nil {
			message = &a2a.Message{
				ID: stored.ID, Role: roleFromStored(stored.Role),
				Parts: a2a.ContentParts{a2a.NewRawPart(stored.Data)},
			}
		}
		message.ID = stored.ID
		message.TaskID = task.ID
		message.ContextID = task.ContextID
		history = append(history, message)
	}
	task.History = truncateHistory(history, historyLength)

	if includeArtifacts && snapshot.Task.LatestTurn > 0 {
		artifact, ok := latestArtifact(snapshot)
		if !ok {
			return nil, fmt.Errorf("task %q latest turn %d has no live artifact",
				snapshot.Task.ID, snapshot.Task.LatestTurn)
		}
		if artifact.MediaType != delegation.GitPatchMediaType {
			return nil, fmt.Errorf("task %q artifact %q has unsupported media type %q",
				snapshot.Task.ID, artifact.ID, artifact.MediaType)
		}
		metadata := decodeMetadata(artifact.Metadata)
		metadata[MetadataKey] = map[string]any{
			"turn":    artifact.TurnNumber,
			"sha256":  artifact.SHA256,
			"replace": true,
		}
		part := a2a.NewRawPart(artifact.Data)
		part.MediaType = artifact.MediaType
		task.Artifacts = []*a2a.Artifact{{
			ID:          a2a.ArtifactID(artifact.ID),
			Name:        "cumulative-delivery",
			Description: "Latest cumulative delegation artifact (replace semantics)",
			Metadata:    metadata,
			Parts:       a2a.ContentParts{part},
		}}
	}
	return task, nil
}

func toA2AState(state delegation.State) a2a.TaskState {
	switch state {
	case delegation.StateSubmitted:
		return a2a.TaskStateSubmitted
	case delegation.StateWorking:
		return a2a.TaskStateWorking
	case delegation.StateInputRequired, delegation.StateRewindPending:
		return a2a.TaskStateInputRequired
	case delegation.StateCompleted:
		return a2a.TaskStateCompleted
	case delegation.StateCanceled:
		return a2a.TaskStateCanceled
	case delegation.StateRejected:
		return a2a.TaskStateRejected
	case delegation.StateFailed:
		return a2a.TaskStateFailed
	default:
		return a2a.TaskStateUnspecified
	}
}

func latestArtifact(snapshot delegation.Snapshot) (delegation.Artifact, bool) {
	for _, artifact := range snapshot.Artifacts {
		if artifact.TurnNumber == snapshot.Task.LatestTurn && !artifact.Superseded() {
			return artifact, true
		}
	}
	return delegation.Artifact{}, false
}

func truncateHistory(history []*a2a.Message, historyLength *int) []*a2a.Message {
	if historyLength == nil || *historyLength < 0 {
		return history
	}
	if *historyLength == 0 {
		return []*a2a.Message{}
	}
	if *historyLength < len(history) {
		return history[len(history)-*historyLength:]
	}
	return history
}

func decodeMetadata(data []byte) map[string]any {
	metadata := make(map[string]any)
	if len(data) == 0 {
		return metadata
	}
	if err := json.Unmarshal(data, &metadata); err != nil || metadata == nil {
		return map[string]any{
			opaqueMetadataKey: base64.StdEncoding.EncodeToString(data),
		}
	}
	return metadata
}

func roleFromStored(role string) a2a.MessageRole {
	switch role {
	case string(a2a.MessageRoleAgent):
		return a2a.MessageRoleAgent
	default:
		return a2a.MessageRoleUser
	}
}
