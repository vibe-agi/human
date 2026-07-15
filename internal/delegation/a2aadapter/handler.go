package a2aadapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/vibe-agi/human/internal/delegation"
)

// Authority is the durable single source of truth behind the A2A projection.
// The adapter deliberately implements a2asrv.RequestHandler directly instead
// of introducing the SDK executor/taskstore as a second task authority.
type Authority interface {
	CreateTaskWithMessage(context.Context, delegation.CreateTaskInput, delegation.MessageInput) (delegation.MessageResult, error)
	AppendMessage(context.Context, delegation.CommandInput, delegation.MessageInput) (delegation.MessageResult, error)
	LoadSnapshot(context.Context, string) (delegation.Snapshot, error)
	ListTasks(context.Context, string) ([]delegation.Task, error)
	CancelTask(context.Context, delegation.CommandInput) (delegation.TransitionResult, error)
	ResolveExec(context.Context, delegation.ResolveExecInput) (delegation.ExecResult, error)
}

// requestHandler contains no lifecycle state of its own. Every response is
// rebuilt from an Authority snapshot, including after a human-side update.
type requestHandler struct {
	authority  Authority
	remoteExec bool
}

var _ a2asrv.RequestHandler = (*requestHandler)(nil)

func (handler *requestHandler) SendMessage(
	ctx context.Context,
	req *a2a.SendMessageRequest,
) (a2a.SendMessageResult, error) {
	callerID, ok := callerFromContext(ctx)
	if !ok {
		return nil, a2a.ErrUnauthenticated
	}
	if err := validateSendMessage(req); err != nil {
		return nil, err
	}
	if req.Config != nil && req.Config.PushConfig != nil {
		return nil, a2a.ErrPushNotificationNotSupported
	}
	if req.Config != nil && len(req.Config.AcceptedOutputModes) > 0 &&
		!acceptsOutputMode(req.Config.AcceptedOutputModes, delegation.GitPatchMediaType) {
		return nil, fmt.Errorf("caller does not accept %s: %w",
			delegation.GitPatchMediaType, a2a.ErrUnsupportedContentType)
	}
	messageData, err := json.Marshal(req.Message)
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", a2a.ErrInvalidParams)
	}
	messageInput := delegation.MessageInput{
		ID: req.Message.ID, Role: string(req.Message.Role), Data: messageData,
	}
	if hasExecResolutionIntent(req.Metadata) {
		if !handler.remoteExec {
			return nil, a2a.ErrUnsupportedOperation
		}
		if req.Message.TaskID == "" {
			return nil, fmt.Errorf("command result requires a task id: %w", a2a.ErrInvalidParams)
		}
		owned, err := handler.loadOwnedSnapshot(ctx, callerID, string(req.Message.TaskID))
		if err != nil {
			return nil, err
		}
		if req.Message.ContextID != "" && req.Message.ContextID != owned.Task.ContextID {
			return nil, fmt.Errorf("message context does not match task: %w", a2a.ErrInvalidParams)
		}
		resolution, ok, err := decodeExecResolution(req.Metadata, req.Message.ID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("command result metadata is invalid: %w", a2a.ErrInvalidParams)
		}
		return handler.resolveExec(ctx, callerID, req, resolution, messageData)
	}

	if req.Message.TaskID == "" {
		metadata, err := encodeRequestMetadata(req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("encode request metadata: %w", a2a.ErrInvalidParams)
		}
		requestedContextID := strings.TrimSpace(req.Message.ContextID)
		contextID := requestedContextID
		if contextID == "" {
			contextID = a2a.NewContextID()
		}
		var result delegation.MessageResult
		for attempt := 0; attempt < 4; attempt++ {
			result, err = handler.authority.CreateTaskWithMessage(ctx, delegation.CreateTaskInput{
				ID: string(a2a.NewTaskID()), CallerID: callerID,
				ContextID: contextID, Metadata: metadata,
			}, messageInput)
			if !errors.Is(err, delegation.ErrAlreadyExists) {
				break
			}
		}
		if err != nil {
			return nil, toProtocolError(err)
		}
		if result.Replay && ((requestedContextID != "" && result.Task.ContextID != requestedContextID) ||
			!bytes.Equal(result.Task.Metadata, metadata)) {
			return nil, toProtocolError(fmt.Errorf(
				"%w: message %q was already used with different task parameters",
				delegation.ErrIdempotencyConflict, req.Message.ID,
			))
		}
		snapshot, err := handler.loadOwnedSnapshot(ctx, callerID, result.Task.ID)
		if err != nil {
			return nil, err
		}
		return toA2ATask(snapshot, historyLength(req), true, handler.remoteExec)
	}

	taskID := string(req.Message.TaskID)
	for attempt := 0; attempt < 4; attempt++ {
		snapshot, err := handler.loadOwnedSnapshot(ctx, callerID, taskID)
		if err != nil {
			return nil, err
		}
		if req.Message.ContextID != "" && req.Message.ContextID != snapshot.Task.ContextID {
			return nil, fmt.Errorf("message context does not match task: %w", a2a.ErrInvalidParams)
		}
		result, err := handler.authority.AppendMessage(ctx, delegation.CommandInput{
			TaskID: taskID, ExpectedRevision: snapshot.Task.Revision,
		}, messageInput)
		if errors.Is(err, delegation.ErrRevisionConflict) {
			continue
		}
		if err != nil {
			return nil, toProtocolError(err)
		}
		snapshot, err = handler.loadOwnedSnapshot(ctx, callerID, result.Task.ID)
		if err != nil {
			return nil, err
		}
		return toA2ATask(snapshot, historyLength(req), true, handler.remoteExec)
	}
	return nil, fmt.Errorf("task kept changing while appending message: %w", a2a.ErrServerError)
}

type execResolutionWire struct {
	Intent       string `json:"intent"`
	RequestID    string `json:"requestId"`
	ResolutionID string `json:"resolutionId"`
	Approved     *bool  `json:"approved"`
	ExitCode     int    `json:"exitCode"`
	StdoutBase64 string `json:"stdoutBase64"`
	StderrBase64 string `json:"stderrBase64"`
	Error        string `json:"error"`
	Truncated    bool   `json:"truncated"`
	TimedOut     bool   `json:"timedOut"`
}

func hasExecResolutionIntent(metadata map[string]any) bool {
	value, ok := metadata[RequestMetadataKey]
	if !ok {
		return false
	}
	object, ok := value.(map[string]any)
	if !ok {
		return false
	}
	intent, ok := object["intent"].(string)
	return ok && strings.TrimSpace(intent) == "command_result"
}

func decodeExecResolution(metadata map[string]any, messageID string) (delegation.ResolveExecInput, bool, error) {
	value, ok := metadata[RequestMetadataKey]
	if !ok {
		return delegation.ResolveExecInput{}, false, nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return delegation.ResolveExecInput{}, false, fmt.Errorf("encode command result metadata: %w", a2a.ErrInvalidParams)
	}
	var wire execResolutionWire
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return delegation.ResolveExecInput{}, false, fmt.Errorf("decode command result metadata: %w", a2a.ErrInvalidParams)
	}
	if wire.Intent != "command_result" {
		return delegation.ResolveExecInput{}, false, nil
	}
	if wire.RequestID == "" || wire.RequestID != strings.TrimSpace(wire.RequestID) ||
		wire.ResolutionID == "" || wire.ResolutionID != strings.TrimSpace(wire.ResolutionID) ||
		wire.ResolutionID != messageID || wire.Approved == nil || wire.Error != strings.TrimSpace(wire.Error) {
		return delegation.ResolveExecInput{}, false, fmt.Errorf("command result identity is invalid: %w", a2a.ErrInvalidParams)
	}
	if !*wire.Approved && wire.ExitCode != 0 {
		return delegation.ResolveExecInput{}, false, fmt.Errorf("denied command cannot carry an exit code: %w", a2a.ErrInvalidParams)
	}
	if base64.StdEncoding.DecodedLen(len(wire.StdoutBase64)) > 8<<20 ||
		base64.StdEncoding.DecodedLen(len(wire.StderrBase64)) > 8<<20 {
		return delegation.ResolveExecInput{}, false, fmt.Errorf("command output exceeds protocol limits: %w", a2a.ErrInvalidParams)
	}
	stdout, err := base64.StdEncoding.DecodeString(wire.StdoutBase64)
	if err != nil {
		return delegation.ResolveExecInput{}, false, fmt.Errorf("command stdout is not base64: %w", a2a.ErrInvalidParams)
	}
	stderr, err := base64.StdEncoding.DecodeString(wire.StderrBase64)
	if err != nil {
		return delegation.ResolveExecInput{}, false, fmt.Errorf("command stderr is not base64: %w", a2a.ErrInvalidParams)
	}
	return delegation.ResolveExecInput{
		RequestID: wire.RequestID, ResolutionID: wire.ResolutionID, Approved: *wire.Approved,
		ExitCode: wire.ExitCode, Stdout: stdout, Stderr: stderr, Error: wire.Error,
		Truncated: wire.Truncated, TimedOut: wire.TimedOut,
	}, true, nil
}

func (handler *requestHandler) resolveExec(
	ctx context.Context,
	callerID string,
	req *a2a.SendMessageRequest,
	input delegation.ResolveExecInput,
	messageData []byte,
) (a2a.SendMessageResult, error) {
	taskID := string(req.Message.TaskID)
	for attempt := 0; attempt < 4; attempt++ {
		snapshot, err := handler.loadOwnedSnapshot(ctx, callerID, taskID)
		if err != nil {
			return nil, err
		}
		if req.Message.ContextID != "" && req.Message.ContextID != snapshot.Task.ContextID {
			return nil, fmt.Errorf("message context does not match task: %w", a2a.ErrInvalidParams)
		}
		input.CommandInput = delegation.CommandInput{
			TaskID: taskID, ExpectedRevision: snapshot.Task.Revision, Data: messageData,
		}
		result, err := handler.authority.ResolveExec(ctx, input)
		if errors.Is(err, delegation.ErrRevisionConflict) {
			continue
		}
		if err != nil {
			if errors.Is(err, delegation.ErrNotFound) {
				return nil, fmt.Errorf("command request not found: %w", a2a.ErrInvalidParams)
			}
			return nil, toProtocolError(err)
		}
		snapshot, err = handler.loadOwnedSnapshot(ctx, callerID, result.Task.ID)
		if err != nil {
			return nil, err
		}
		return toA2ATask(snapshot, historyLength(req), true, handler.remoteExec)
	}
	return nil, fmt.Errorf("task kept changing while resolving command: %w", a2a.ErrServerError)
}

func (handler *requestHandler) GetTask(
	ctx context.Context,
	req *a2a.GetTaskRequest,
) (*a2a.Task, error) {
	callerID, ok := callerFromContext(ctx)
	if !ok {
		return nil, a2a.ErrUnauthenticated
	}
	if req == nil || req.ID == "" {
		return nil, a2a.ErrInvalidParams
	}
	snapshot, err := handler.loadOwnedSnapshot(ctx, callerID, string(req.ID))
	if err != nil {
		return nil, err
	}
	return toA2ATask(snapshot, req.HistoryLength, true, handler.remoteExec)
}

func (handler *requestHandler) ListTasks(
	ctx context.Context,
	req *a2a.ListTasksRequest,
) (*a2a.ListTasksResponse, error) {
	callerID, ok := callerFromContext(ctx)
	if !ok {
		return nil, a2a.ErrUnauthenticated
	}
	if req == nil {
		return nil, a2a.ErrInvalidParams
	}
	pageSize := req.PageSize
	if pageSize == 0 {
		pageSize = 50
	} else if pageSize < 1 || pageSize > 100 {
		return nil, fmt.Errorf("page size must be between 1 and 100: %w", a2a.ErrInvalidRequest)
	}
	if req.Status != a2a.TaskStateUnspecified && !knownA2AState(req.Status) {
		return nil, fmt.Errorf("unknown task state %q: %w", req.Status, a2a.ErrInvalidRequest)
	}
	cursor, err := decodePageToken(req.PageToken)
	if err != nil {
		return nil, err
	}
	tasks, err := handler.authority.ListTasks(ctx, callerID)
	if err != nil {
		return nil, toProtocolError(err)
	}
	filtered := make([]delegation.Task, 0, len(tasks))
	for _, task := range tasks {
		if req.ContextID != "" && task.ContextID != req.ContextID {
			continue
		}
		if req.Status != a2a.TaskStateUnspecified && toA2AState(task.State) != req.Status {
			continue
		}
		if req.StatusTimestampAfter != nil && !task.UpdatedAt.After(*req.StatusTimestampAfter) {
			continue
		}
		filtered = append(filtered, task)
	}
	totalSize := len(filtered)
	page := filtered
	if cursor != nil {
		page = make([]delegation.Task, 0, len(filtered))
		for _, task := range filtered {
			if taskAfterCursor(task, *cursor) {
				page = append(page, task)
			}
		}
	}
	var nextPageToken string
	if len(page) > pageSize {
		page = page[:pageSize]
		nextPageToken, err = encodePageToken(page[len(page)-1])
		if err != nil {
			return nil, fmt.Errorf("encode page token: %w", a2a.ErrInternalError)
		}
	}
	historyLimit := req.HistoryLength
	if historyLimit == nil {
		defaultHistoryLength := 100
		historyLimit = &defaultHistoryLength
	}
	result := make([]*a2a.Task, 0, len(page))
	for _, task := range page {
		snapshot, err := handler.loadOwnedSnapshot(ctx, callerID, task.ID)
		if err != nil {
			return nil, err
		}
		mapped, err := toA2ATask(snapshot, historyLimit, req.IncludeArtifacts, handler.remoteExec)
		if err != nil {
			return nil, fmt.Errorf("map task %q: %w", task.ID, a2a.ErrInternalError)
		}
		result = append(result, mapped)
	}
	return &a2a.ListTasksResponse{
		Tasks: result, TotalSize: totalSize, PageSize: pageSize, NextPageToken: nextPageToken,
	}, nil
}

func (handler *requestHandler) CancelTask(
	ctx context.Context,
	req *a2a.CancelTaskRequest,
) (*a2a.Task, error) {
	callerID, ok := callerFromContext(ctx)
	if !ok {
		return nil, a2a.ErrUnauthenticated
	}
	if req == nil || req.ID == "" {
		return nil, a2a.ErrInvalidParams
	}
	data, err := json.Marshal(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("encode cancellation metadata: %w", a2a.ErrInvalidParams)
	}
	taskID := string(req.ID)
	for attempt := 0; attempt < 4; attempt++ {
		snapshot, err := handler.loadOwnedSnapshot(ctx, callerID, taskID)
		if err != nil {
			return nil, err
		}
		if snapshot.Task.State.IsTerminal() {
			return nil, a2a.ErrTaskNotCancelable
		}
		_, err = handler.authority.CancelTask(ctx, delegation.CommandInput{
			TaskID: taskID, ExpectedRevision: snapshot.Task.Revision, Data: data,
		})
		if errors.Is(err, delegation.ErrRevisionConflict) {
			continue
		}
		if err != nil {
			return nil, toProtocolError(err)
		}
		snapshot, err = handler.loadOwnedSnapshot(ctx, callerID, taskID)
		if err != nil {
			return nil, err
		}
		return toA2ATask(snapshot, nil, true, handler.remoteExec)
	}
	return nil, fmt.Errorf("task kept changing while canceling: %w", a2a.ErrServerError)
}

func (handler *requestHandler) SendStreamingMessage(
	context.Context,
	*a2a.SendMessageRequest,
) iter.Seq2[a2a.Event, error] {
	return unsupportedEvents()
}

func (handler *requestHandler) SubscribeToTask(
	context.Context,
	*a2a.SubscribeToTaskRequest,
) iter.Seq2[a2a.Event, error] {
	return unsupportedEvents()
}

func (*requestHandler) GetTaskPushConfig(
	context.Context,
	*a2a.GetTaskPushConfigRequest,
) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) ListTaskPushConfigs(
	context.Context,
	*a2a.ListTaskPushConfigRequest,
) (*a2a.ListTaskPushConfigResponse, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) CreateTaskPushConfig(context.Context, *a2a.PushConfig) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) DeleteTaskPushConfig(
	context.Context,
	*a2a.DeleteTaskPushConfigRequest,
) error {
	return a2a.ErrPushNotificationNotSupported
}

func (*requestHandler) GetExtendedAgentCard(
	context.Context,
	*a2a.GetExtendedAgentCardRequest,
) (*a2a.AgentCard, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (handler *requestHandler) loadOwnedSnapshot(
	ctx context.Context,
	callerID string,
	taskID string,
) (delegation.Snapshot, error) {
	snapshot, err := handler.authority.LoadSnapshot(ctx, taskID)
	if err != nil {
		return delegation.Snapshot{}, toProtocolError(err)
	}
	// Return not-found instead of permission-denied to avoid task enumeration.
	if snapshot.Task.CallerID != callerID {
		return delegation.Snapshot{}, a2a.ErrTaskNotFound
	}
	return snapshot, nil
}

func validateSendMessage(req *a2a.SendMessageRequest) error {
	switch {
	case req == nil:
		return fmt.Errorf("message send request is required: %w", a2a.ErrInvalidParams)
	case req.Message == nil:
		return fmt.Errorf("message is required: %w", a2a.ErrInvalidParams)
	case strings.TrimSpace(req.Message.ID) == "":
		return fmt.Errorf("message ID is required: %w", a2a.ErrInvalidParams)
	case req.Message.Role != a2a.MessageRoleUser:
		return fmt.Errorf("caller message role must be user: %w", a2a.ErrInvalidParams)
	case len(req.Message.Parts) == 0:
		return fmt.Errorf("message parts are required: %w", a2a.ErrInvalidParams)
	}
	return nil
}

func encodeRequestMetadata(metadata map[string]any) ([]byte, error) {
	// The authority namespace is output-only. Copy caller values so the input
	// map is not mutated and normalize nil/empty metadata to the same encoding.
	sanitized := make(map[string]any, len(metadata))
	for key, value := range metadata {
		if key != MetadataKey {
			sanitized[key] = value
		}
	}
	return json.Marshal(sanitized)
}

func acceptsOutputMode(modes []string, wanted string) bool {
	for _, mode := range modes {
		if strings.EqualFold(strings.TrimSpace(mode), wanted) {
			return true
		}
	}
	return false
}

func historyLength(req *a2a.SendMessageRequest) *int {
	if req.Config == nil {
		return nil
	}
	return req.Config.HistoryLength
}

func toProtocolError(err error) error {
	switch {
	case errors.Is(err, delegation.ErrNotFound):
		return a2a.ErrTaskNotFound
	case errors.Is(err, delegation.ErrInvalidInput),
		errors.Is(err, delegation.ErrInvalidTransition),
		errors.Is(err, delegation.ErrInvalidRewind),
		errors.Is(err, delegation.ErrIdempotencyConflict):
		return fmt.Errorf("%v: %w", err, a2a.ErrInvalidParams)
	case errors.Is(err, delegation.ErrRevisionConflict),
		errors.Is(err, delegation.ErrAlreadyExists):
		return fmt.Errorf("%v: %w", err, a2a.ErrServerError)
	default:
		return fmt.Errorf("%v: %w", err, a2a.ErrInternalError)
	}
}

func unsupportedEvents() iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(nil, a2a.ErrUnsupportedOperation)
	}
}

func knownA2AState(state a2a.TaskState) bool {
	switch state {
	case a2a.TaskStateSubmitted, a2a.TaskStateWorking, a2a.TaskStateInputRequired,
		a2a.TaskStateCompleted, a2a.TaskStateCanceled, a2a.TaskStateRejected,
		a2a.TaskStateFailed:
		return true
	default:
		return false
	}
}

type pageCursor struct {
	UpdatedAt time.Time `json:"updatedAt"`
	TaskID    string    `json:"taskId"`
}

func encodePageToken(task delegation.Task) (string, error) {
	data, err := json.Marshal(pageCursor{UpdatedAt: task.UpdatedAt, TaskID: task.ID})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodePageToken(token string) (*pageCursor, error) {
	if token == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid page token: %w", a2a.ErrParseError)
	}
	var cursor pageCursor
	if err := json.Unmarshal(data, &cursor); err != nil || cursor.TaskID == "" || cursor.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("invalid page token: %w", a2a.ErrParseError)
	}
	return &cursor, nil
}

func taskAfterCursor(task delegation.Task, cursor pageCursor) bool {
	return task.UpdatedAt.Before(cursor.UpdatedAt) ||
		(task.UpdatedAt.Equal(cursor.UpdatedAt) && task.ID < cursor.TaskID)
}
