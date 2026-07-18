package a2a

import (
	"encoding/base64"
	"encoding/json"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

const maxTaskCursorBytes = 2048

type taskCursorEnvelope struct {
	Version int                   `json:"version"`
	Cursor  agent.TaskQueryCursor `json:"cursor"`
}

func encodeTaskCursor(cursor agent.TaskQueryCursor) (string, error) {
	payload, err := json.Marshal(taskCursorEnvelope{Version: 1, Cursor: cursor})
	if err != nil {
		return "", protocolError(sdk.ErrInternalError, "failed to encode task page cursor")
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeTaskCursor(token string) (*agent.TaskQueryCursor, error) {
	if token == "" {
		return nil, nil
	}
	// Reject by encoded size before DecodeString allocates from attacker-controlled
	// input. DecodedLen is an upper bound for raw, unpadded base64.
	if len(token) > base64.RawURLEncoding.EncodedLen(maxTaskCursorBytes) ||
		base64.RawURLEncoding.DecodedLen(len(token)) > maxTaskCursorBytes {
		return nil, protocolError(sdk.ErrInvalidParams, "pageToken is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(payload) > maxTaskCursorBytes {
		return nil, protocolError(sdk.ErrInvalidParams, "pageToken is invalid")
	}
	var envelope taskCursorEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope.Version != 1 {
		return nil, protocolError(sdk.ErrInvalidParams, "pageToken is invalid")
	}
	if !validSQLiteTimestamp(envelope.Cursor.UpdatedAt) ||
		!validDurableIdentity(string(envelope.Cursor.Workspace)) ||
		!validDurableIdentity(string(envelope.Cursor.Task)) {
		return nil, protocolError(sdk.ErrInvalidParams, "pageToken is invalid")
	}
	return &envelope.Cursor, nil
}
