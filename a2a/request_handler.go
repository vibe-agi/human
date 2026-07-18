package a2a

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"iter"
	"mime"
	"strings"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/vibe-agi/human/agent"
)

type requestHandler struct {
	config Config
	poll   time.Duration
}

var _ a2asrv.RequestHandler = (*requestHandler)(nil)

func newRequestHandler(config Config) a2asrv.RequestHandler {
	poll := config.PollInterval
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	return &requestHandler{config: config, poll: poll}
}

func (handler *requestHandler) GetTask(ctx context.Context, request *sdk.GetTaskRequest) (*sdk.Task, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil || request.ID == "" {
		return nil, protocolError(sdk.ErrInvalidParams, "task id is required")
	}
	historyLength, err := normalizeHistoryLength(request.HistoryLength)
	if err != nil {
		return nil, err
	}
	ref, err := handler.config.Agent.ResolveTask(ctx, principal.Authority, agent.TaskID(request.ID))
	if err != nil {
		return nil, mapAgentError(err)
	}
	task, err := handler.config.Agent.GetTask(ctx, ref)
	if err != nil {
		return nil, mapAgentError(err)
	}
	return handler.convertTask(ctx, task, historyLength, true)
}

func (handler *requestHandler) ListTasks(ctx context.Context, request *sdk.ListTasksRequest) (*sdk.ListTasksResponse, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil {
		return nil, protocolError(sdk.ErrInvalidParams, "list request is required")
	}
	limit := request.PageSize
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > agent.MaxPageSize {
		return nil, protocolError(sdk.ErrInvalidParams, "pageSize must be between 1 and 100")
	}
	historyLength, err := normalizeHistoryLength(request.HistoryLength)
	if err != nil {
		return nil, err
	}
	cursor, err := decodeTaskCursor(request.PageToken)
	if err != nil {
		return nil, err
	}
	if request.ContextID != "" && !validDurableIdentity(request.ContextID) {
		return nil, protocolError(sdk.ErrInvalidParams, "contextId is invalid")
	}
	statusTimestampAfter := request.StatusTimestampAfter
	if statusTimestampAfter == nil {
		if value, ok := listTimestampFromContext(ctx); ok {
			statusTimestampAfter = &value
		}
	}
	if statusTimestampAfter != nil && !validSQLiteTimestamp(*statusTimestampAfter) {
		return nil, protocolError(sdk.ErrInvalidParams, "statusTimestampAfter is invalid")
	}
	if request.Status == sdk.TaskStateAuthRequired {
		// AUTH_REQUIRED is a valid A2A 1.0 filter, but HumanAgent's durable
		// domain deliberately has no corresponding state. Preserve all request
		// validation above, then return the truthful empty projection.
		return &sdk.ListTasksResponse{Tasks: []*sdk.Task{}, PageSize: limit}, nil
	}
	state, err := fromA2AState(request.Status)
	if err != nil {
		return nil, err
	}
	query := agent.TaskQuery{
		Context: agent.ContextID(request.ContextID), State: state,
		After: cursor, Limit: limit,
	}
	if statusTimestampAfter != nil {
		updated := statusTimestampAfter.UTC()
		query.UpdatedAtOrAfter = &updated
	}
	page, err := handler.config.Agent.ListAuthorityTasks(ctx, principal.Authority, query)
	if err != nil {
		return nil, mapAgentError(err)
	}
	result := &sdk.ListTasksResponse{
		Tasks: make([]*sdk.Task, 0, len(page.Items)), PageSize: limit,
	}
	maxInt := uint64(^uint(0) >> 1)
	if page.TotalSize > maxInt {
		return nil, protocolError(sdk.ErrInternalError, "task count exceeds protocol integer range")
	}
	result.TotalSize = int(page.TotalSize)
	for _, task := range page.Items {
		converted, err := handler.convertTask(ctx, task, historyLength, request.IncludeArtifacts)
		if err != nil {
			return nil, err
		}
		result.Tasks = append(result.Tasks, converted)
	}
	if page.Next != nil {
		result.NextPageToken, err = encodeTaskCursor(*page.Next)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (handler *requestHandler) CancelTask(ctx context.Context, request *sdk.CancelTaskRequest) (*sdk.Task, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil || request.ID == "" {
		return nil, protocolError(sdk.ErrInvalidParams, "task id is required")
	}
	ref, err := handler.config.Agent.ResolveTask(ctx, principal.Authority, agent.TaskID(request.ID))
	if err != nil {
		return nil, mapAgentError(err)
	}
	canceled, err := cancelTaskWithRetry(
		ctx,
		ref,
		deterministicID("a2a-cancel", principal.Authority, string(request.ID)),
		handler.config.Agent.GetTask,
		handler.config.Agent.CancelTask,
	)
	if err != nil {
		return nil, err
	}
	return handler.convertTask(ctx, canceled, nil, true)
}

func (handler *requestHandler) SendMessage(ctx context.Context, request *sdk.SendMessageRequest) (sdk.SendMessageResult, error) {
	task, err := handler.acceptMessage(ctx, request)
	if err != nil {
		return nil, err
	}
	historyLength, err := sendHistoryLength(request)
	if err != nil {
		return nil, err
	}
	if request.Config != nil && request.Config.ReturnImmediately {
		return handler.convertTask(ctx, task, historyLength, true)
	}
	task, err = handler.waitUntilPaused(ctx, task.Ref)
	if err != nil {
		return nil, err
	}
	return handler.convertTask(ctx, task, historyLength, true)
}

func (handler *requestHandler) SendStreamingMessage(ctx context.Context, request *sdk.SendMessageRequest) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		task, err := handler.acceptMessage(ctx, request)
		if err != nil {
			yield(nil, err)
			return
		}
		historyLength, err := sendHistoryLength(request)
		if err != nil {
			yield(nil, err)
			return
		}
		initial, err := handler.convertTask(ctx, task, historyLength, true)
		if err != nil || !yield(initial, err) || task.State.Terminal() || task.State == agent.TaskInputRequired {
			return
		}
		handler.tailEvents(ctx, task.Ref, task.EventCount, true, yield)
	}
}

func (handler *requestHandler) SubscribeToTask(ctx context.Context, request *sdk.SubscribeToTaskRequest) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		principal, err := requirePrincipal(ctx)
		if err != nil {
			yield(nil, err)
			return
		}
		if request == nil || request.ID == "" {
			yield(nil, protocolError(sdk.ErrInvalidParams, "task id is required"))
			return
		}
		ref, err := handler.config.Agent.ResolveTask(ctx, principal.Authority, agent.TaskID(request.ID))
		if err != nil {
			yield(nil, mapAgentError(err))
			return
		}
		snapshot, err := handler.config.Agent.SnapshotTask(ctx, ref)
		if err != nil {
			yield(nil, mapAgentError(err))
			return
		}
		if snapshot.Task.State.Terminal() {
			yield(nil, protocolError(sdk.ErrUnsupportedOperation, "terminal task cannot be subscribed"))
			return
		}
		initial, err := handler.convertTask(ctx, snapshot.Task, nil, true)
		if err != nil || !yield(initial, err) || snapshot.Task.State == agent.TaskInputRequired {
			return
		}
		handler.tailEvents(ctx, ref, snapshot.EventCursor, true, yield)
	}
}

func (handler *requestHandler) GetTaskPushConfig(context.Context, *sdk.GetTaskPushConfigRequest) (*sdk.PushConfig, error) {
	return nil, protocolError(sdk.ErrPushNotificationNotSupported, "push notifications are not supported")
}

func (handler *requestHandler) ListTaskPushConfigs(context.Context, *sdk.ListTaskPushConfigRequest) (*sdk.ListTaskPushConfigResponse, error) {
	return nil, protocolError(sdk.ErrPushNotificationNotSupported, "push notifications are not supported")
}

func (handler *requestHandler) CreateTaskPushConfig(context.Context, *sdk.PushConfig) (*sdk.PushConfig, error) {
	return nil, protocolError(sdk.ErrPushNotificationNotSupported, "push notifications are not supported")
}

func (handler *requestHandler) DeleteTaskPushConfig(context.Context, *sdk.DeleteTaskPushConfigRequest) error {
	return protocolError(sdk.ErrPushNotificationNotSupported, "push notifications are not supported")
}

func (handler *requestHandler) GetExtendedAgentCard(context.Context, *sdk.GetExtendedAgentCardRequest) (*sdk.AgentCard, error) {
	return nil, protocolError(sdk.ErrExtendedCardNotConfigured, "extended Agent Card is not configured")
}

func (handler *requestHandler) acceptMessage(ctx context.Context, request *sdk.SendMessageRequest) (agent.Task, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return agent.Task{}, err
	}
	if request == nil || request.Message == nil {
		return agent.Task{}, protocolError(sdk.ErrInvalidParams, "message is required")
	}
	if request.Config != nil {
		if request.Config.PushConfig != nil {
			return agent.Task{}, protocolError(sdk.ErrPushNotificationNotSupported, "push notifications are not supported")
		}
		if err := validateHistoryLength(request.Config.HistoryLength); err != nil {
			return agent.Task{}, err
		}
		if err := validateAcceptedOutputModes(
			handler.config.Card.DefaultOutputModes,
			request.Config.AcceptedOutputModes,
		); err != nil {
			return agent.Task{}, err
		}
	}
	message, err := fromA2AMessage(request.Message)
	if err != nil {
		return agent.Task{}, err
	}
	if err := validatePartModes(handler.config.Card.DefaultInputModes, message.Parts, "input"); err != nil {
		return agent.Task{}, err
	}
	if request.Message.TaskID == "" {
		if replay, found, err := handler.existingInitialMessage(
			ctx, principal.Authority, request.Message.ContextID, message,
		); err != nil {
			return agent.Task{}, err
		} else if found {
			return replay, nil
		}
		if handler.config.ResolveWorkspace == nil {
			return agent.Task{}, protocolError(sdk.ErrInternalError, "workspace resolver is not configured")
		}
		workspaceID, err := handler.config.ResolveWorkspace(ctx, principal, request)
		if err != nil {
			return agent.Task{}, mapResolverError(err)
		}
		contextID := request.Message.ContextID
		if contextID == "" {
			contextID = string(deterministicID("a2a-context", principal.Authority, request.Message.ID))
		}
		ref := agent.TaskRef{
			Workspace: agent.WorkspaceRef{Authority: principal.Authority, ID: workspaceID},
			ID:        agent.TaskID(deterministicID("a2a-task", principal.Authority, request.Message.ID)),
		}
		created, err := handler.config.Agent.CreateTask(ctx, agent.CreateTaskCommand{
			Meta: agent.CommandMeta{ID: deterministicID("a2a-create", principal.Authority, request.Message.ID)},
			Task: ref, Context: agent.ContextRef{Authority: principal.Authority, ID: agent.ContextID(contextID)},
			Message: message,
		})
		if err != nil {
			// Two taskless retries can both miss the message before either Create
			// commits, and a resolver may change while they race. The first durable
			// message wins; recognize it exactly instead of surfacing the losing
			// resolver's command digest conflict.
			if replay, found, replayErr := handler.existingInitialMessage(
				ctx, principal.Authority, request.Message.ContextID, message,
			); replayErr != nil {
				return agent.Task{}, replayErr
			} else if found {
				return replay, nil
			}
			return agent.Task{}, mapAgentError(err)
		}
		current, err := handler.config.Agent.GetTask(ctx, created.Ref)
		if err != nil {
			return agent.Task{}, mapAgentError(err)
		}
		return current, nil
	}

	ref, err := handler.config.Agent.ResolveTask(ctx, principal.Authority, agent.TaskID(request.Message.TaskID))
	if err != nil {
		return agent.Task{}, mapAgentError(err)
	}
	current, err := handler.config.Agent.GetTask(ctx, ref)
	if err != nil {
		return agent.Task{}, mapAgentError(err)
	}
	if request.Message.ContextID != "" && agent.ContextID(request.Message.ContextID) != current.Context.ID {
		return agent.Task{}, protocolError(sdk.ErrInvalidParams, "message context does not match task")
	}
	if replay, found, err := handler.existingCallerMessage(ctx, principal.Authority, ref, message); err != nil {
		return agent.Task{}, err
	} else if found {
		return replay, nil
	}
	if current.State != agent.TaskInputRequired {
		return agent.Task{}, protocolError(sdk.ErrUnsupportedOperation, "task is not waiting for caller input")
	}
	_, err = handler.config.Agent.ReplyTask(ctx, agent.MessageCommand{
		Meta: agent.CommandMeta{
			ID:               deterministicID("a2a-message", principal.Authority, request.Message.ID),
			ExpectedRevision: current.Revision,
		},
		Task: ref, Message: message,
	})
	if err != nil {
		if replay, found, replayErr := handler.existingCallerMessage(ctx, principal.Authority, ref, message); replayErr != nil {
			return agent.Task{}, replayErr
		} else if found {
			return replay, nil
		}
		return agent.Task{}, mapAgentError(err)
	}
	current, err = handler.config.Agent.GetTask(ctx, ref)
	if err != nil {
		return agent.Task{}, mapAgentError(err)
	}
	return current, nil
}

func (handler *requestHandler) existingInitialMessage(
	ctx context.Context,
	authority agent.AuthorityID,
	contextID string,
	want agent.MessageInput,
) (agent.Task, bool, error) {
	stored, err := handler.config.Agent.GetMessage(ctx, authority, want.ID)
	if errors.Is(err, agent.ErrNotFound) {
		return agent.Task{}, false, nil
	}
	if err != nil {
		return agent.Task{}, false, mapAgentError(err)
	}
	wantTaskID := agent.TaskID(deterministicID("a2a-task", authority, string(want.ID)))
	if stored.Author != agent.AuthorCaller || stored.Sequence != 1 || stored.Task.ID != wantTaskID ||
		!sameParts(stored.Parts, want.Parts) {
		return agent.Task{}, false, protocolError(sdk.ErrInvalidParams, "message id was reused with different content")
	}
	current, err := handler.config.Agent.GetTask(ctx, stored.Task)
	if err != nil {
		return agent.Task{}, false, mapAgentError(err)
	}
	expectedContext := agent.ContextID(contextID)
	if expectedContext == "" {
		expectedContext = agent.ContextID(deterministicID("a2a-context", authority, string(want.ID)))
	}
	if current.Context.ID != expectedContext {
		return agent.Task{}, false, protocolError(sdk.ErrInvalidParams, "message context does not match task")
	}
	return current, true, nil
}

func (handler *requestHandler) existingCallerMessage(
	ctx context.Context,
	authority agent.AuthorityID,
	ref agent.TaskRef,
	want agent.MessageInput,
) (agent.Task, bool, error) {
	stored, err := handler.config.Agent.GetMessage(ctx, authority, want.ID)
	if errors.Is(err, agent.ErrNotFound) {
		return agent.Task{}, false, nil
	}
	if err != nil {
		return agent.Task{}, false, mapAgentError(err)
	}
	if stored.Task != ref || stored.Author != agent.AuthorCaller || !sameParts(stored.Parts, want.Parts) {
		return agent.Task{}, false, protocolError(sdk.ErrInvalidParams, "message id was reused with different content")
	}
	current, err := handler.config.Agent.GetTask(ctx, ref)
	if err != nil {
		return agent.Task{}, false, mapAgentError(err)
	}
	return current, true, nil
}

func (handler *requestHandler) waitUntilPaused(ctx context.Context, ref agent.TaskRef) (agent.Task, error) {
	ticker := time.NewTicker(handler.poll)
	defer ticker.Stop()
	for {
		task, err := handler.config.Agent.GetTask(ctx, ref)
		if err != nil {
			return agent.Task{}, mapAgentError(err)
		}
		if task.State.Terminal() || task.State == agent.TaskInputRequired {
			return task, nil
		}
		select {
		case <-ctx.Done():
			return agent.Task{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func requirePrincipal(ctx context.Context) (Principal, error) {
	principal, ok := PrincipalFromContext(ctx)
	if !ok || principal.Authority == "" {
		return Principal{}, protocolError(sdk.ErrUnauthenticated, "authenticated principal is missing")
	}
	return principal, nil
}

func validateHistoryLength(value *int) error {
	if value != nil && *value < 0 {
		return protocolError(sdk.ErrInvalidParams, "historyLength must not be negative")
	}
	return nil
}

func normalizeHistoryLength(value *int) (*int, error) {
	if err := validateHistoryLength(value); err != nil || value == nil {
		return nil, err
	}
	normalized := min(*value, agent.MaxPageSize)
	return &normalized, nil
}

func sendHistoryLength(request *sdk.SendMessageRequest) (*int, error) {
	if request == nil || request.Config == nil {
		return nil, nil
	}
	return normalizeHistoryLength(request.Config.HistoryLength)
}

func validateAcceptedOutputModes(declared, accepted []string) error {
	if len(accepted) == 0 {
		return nil
	}
	requested := make(map[string]struct{}, len(accepted))
	for _, value := range accepted {
		mode, ok := normalizedMediaMode(value)
		if !ok {
			return protocolError(sdk.ErrInvalidParams, "acceptedOutputModes contains an invalid media type")
		}
		requested[mode] = struct{}{}
	}
	for _, value := range declared {
		mode, ok := normalizedMediaMode(value)
		if !ok {
			return protocolError(sdk.ErrInternalError, "Agent Card contains an invalid output media type")
		}
		if _, accepted := requested[mode]; !accepted {
			return protocolError(
				sdk.ErrUnsupportedContentType,
				"acceptedOutputModes must cover every output mode advertised by this Agent",
			)
		}
	}
	return nil
}

func validatePartModes(declared []string, parts []agent.Part, direction string) error {
	available := make(map[string]struct{}, len(declared))
	for _, value := range declared {
		mode, ok := normalizedMediaMode(value)
		if ok {
			available[mode] = struct{}{}
		}
	}
	for _, part := range parts {
		mode, ok := normalizedMediaMode(part.MediaType)
		if !ok {
			return protocolError(sdk.ErrInternalError, direction+" Part has an invalid media type")
		}
		if _, supported := available[mode]; !supported {
			return protocolError(
				sdk.ErrUnsupportedContentType,
				direction+" Part media type is not declared by this Agent",
			)
		}
	}
	return nil
}

func normalizedMediaMode(value string) (string, bool) {
	if value == "" || value != strings.TrimSpace(value) {
		return "", false
	}
	base, _, err := mime.ParseMediaType(value)
	if err != nil {
		return "", false
	}
	return strings.ToLower(base), true
}

type loadTaskFunc func(context.Context, agent.TaskRef) (agent.Task, error)
type cancelTaskFunc func(context.Context, agent.TaskCommand) (agent.Task, error)

func cancelTaskWithRetry(
	ctx context.Context,
	ref agent.TaskRef,
	commandID agent.CommandID,
	load loadTaskFunc,
	cancel cancelTaskFunc,
) (agent.Task, error) {
	for {
		current, err := load(ctx, ref)
		if err != nil {
			return agent.Task{}, mapAgentError(err)
		}
		switch {
		case current.State == agent.TaskCanceled:
			return current, nil
		case current.State.Terminal():
			return agent.Task{}, protocolError(sdk.ErrTaskNotCancelable, "task is already terminal")
		}

		canceled, err := cancel(ctx, agent.TaskCommand{
			Meta: agent.CommandMeta{ID: commandID, ExpectedRevision: current.Revision},
			Task: ref,
		})
		if err == nil {
			return canceled, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return agent.Task{}, err
		}
		if errors.Is(err, agent.ErrRevisionConflict) {
			continue
		}

		var transition *agent.TransitionError
		if errors.As(err, &transition) {
			// A transition error can race with a terminal transition. Reload once so
			// canceled remains idempotent while every other terminal state is
			// reported accurately as not cancelable.
			latest, loadErr := load(ctx, ref)
			if loadErr != nil {
				return agent.Task{}, mapAgentError(loadErr)
			}
			if latest.State == agent.TaskCanceled {
				return latest, nil
			}
			if latest.State.Terminal() {
				return agent.Task{}, protocolError(sdk.ErrTaskNotCancelable, "task is already terminal")
			}
		}
		return agent.Task{}, mapCancelError(err)
	}
}

func deterministicID(prefix string, authority agent.AuthorityID, identity string) agent.CommandID {
	digest := sha256.Sum256([]byte(prefix + "\x00" + string(authority) + "\x00" + identity))
	return agent.CommandID(prefix + "-" + hex.EncodeToString(digest[:]))
}
