package humanmcp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/vibe-agi/human/internal/delegation"
)

// A2AConfig connects the local MCP process to the public Agent Card and the
// authenticated A2A endpoint it advertises. By default every advertised
// interface must share the base URL's scheme and authority, preventing a
// public card from redirecting the caller bearer token to another origin.
type A2AConfig struct {
	BaseURL           string
	BearerToken       string
	HTTPClient        *http.Client
	AllowCrossOrigin  bool
	AllowInsecureHTTP bool
}

type protocolClient interface {
	SendMessage(context.Context, *a2a.SendMessageRequest) (a2a.SendMessageResult, error)
	GetTask(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error)
	ListTasks(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error)
	CancelTask(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error)
}

// A2AAuthority implements Authority using the official A2A Go SDK.
type A2AAuthority struct {
	client protocolClient
}

func NewA2AAuthority(ctx context.Context, config A2AConfig) (*A2AAuthority, error) {
	baseURL := strings.TrimSpace(config.BaseURL)
	token := strings.TrimSpace(config.BearerToken)
	if baseURL == "" || token == "" {
		return nil, errors.New("A2A base URL and bearer token are required")
	}
	parsedBase, err := url.Parse(baseURL)
	if err != nil || parsedBase.Scheme == "" || parsedBase.Host == "" {
		return nil, fmt.Errorf("invalid A2A base URL %q", baseURL)
	}
	if parsedBase.Scheme != "https" && parsedBase.Scheme != "http" {
		return nil, fmt.Errorf("unsupported A2A URL scheme %q", parsedBase.Scheme)
	}
	if parsedBase.Scheme == "http" && !loopbackHost(parsedBase.Hostname()) && !config.AllowInsecureHTTP {
		return nil, errors.New("plaintext A2A HTTP is allowed only on loopback; use HTTPS or explicit insecure opt-in")
	}
	plainClient := cloneHTTPClient(config.HTTPClient)
	card, err := agentcard.NewResolver(plainClient).Resolve(ctx, baseURL)
	if err != nil {
		return nil, fmt.Errorf("resolve A2A Agent Card: %w", err)
	}
	if err := validateCardInterfaces(parsedBase, card, config.AllowCrossOrigin, config.AllowInsecureHTTP); err != nil {
		return nil, err
	}
	authorizedClient := cloneHTTPClient(plainClient)
	baseTransport := authorizedClient.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	authorizedClient.Transport = bearerTransport{base: baseTransport, token: token}
	previousRedirect := authorizedClient.CheckRedirect
	authorizedClient.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > 0 && !sameOrigin(request.URL, via[0].URL) {
			return errors.New("refusing cross-origin A2A redirect")
		}
		if previousRedirect != nil {
			return previousRedirect(request, via)
		}
		return nil
	}
	client, err := a2aclient.NewFromCard(
		ctx, card,
		a2aclient.WithJSONRPCTransport(authorizedClient),
		a2aclient.WithRESTTransport(authorizedClient),
		a2aclient.WithConfig(a2aclient.Config{
			AcceptedOutputModes: []string{delegation.GitPatchMediaType},
			PreferredTransports: []a2a.TransportProtocol{
				a2a.TransportProtocolJSONRPC, a2a.TransportProtocolHTTPJSON,
			},
			DisableTenantPropagation: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create A2A client: %w", err)
	}
	return &A2AAuthority{client: client}, nil
}

func newA2AAuthority(client protocolClient) *A2AAuthority {
	return &A2AAuthority{client: client}
}

func (authority *A2AAuthority) Delegate(ctx context.Context, input DelegateInput) (Task, error) {
	messageID := strings.TrimSpace(input.IdempotencyKey)
	if messageID == "" {
		return Task{}, errors.New("idempotency key is required")
	}
	references := make([]a2a.TaskID, 0, len(input.ReferenceTaskIDs))
	for _, reference := range compactStrings(input.ReferenceTaskIDs) {
		references = append(references, a2a.TaskID(reference))
	}
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(strings.TrimSpace(input.Prompt)))
	message.ID = messageID
	message.ReferenceTasks = references
	result, err := authority.client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: message,
		Config: &a2a.SendMessageConfig{
			ReturnImmediately:   true,
			AcceptedOutputModes: []string{delegation.GitPatchMediaType},
		},
		Metadata: map[string]any{delegation.RequestMetadataKey: map[string]any{
			"baseCommit": input.BaseCommit, "workspaceDigest": input.WorkspaceDigest,
			"execPolicy": input.ExecPolicy, "idempotencyKey": messageID,
		}},
	})
	if err != nil {
		return Task{}, err
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		return Task{}, fmt.Errorf("A2A delegation returned %T instead of a task", result)
	}
	return fromA2ATask(task)
}

func (authority *A2AAuthority) GetTask(ctx context.Context, taskID string) (Task, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return Task{}, errors.New("task id is required")
	}
	task, err := authority.client.GetTask(ctx, &a2a.GetTaskRequest{ID: a2a.TaskID(taskID)})
	if err != nil {
		return Task{}, err
	}
	return fromExpectedA2ATask(task, taskID, "get task")
}

func (authority *A2AAuthority) ListTasks(ctx context.Context) ([]Task, error) {
	var result []Task
	pageToken := ""
	seenTokens := map[string]struct{}{"": {}}
	for page := 0; page < 10_000; page++ {
		response, err := authority.client.ListTasks(ctx, &a2a.ListTasksRequest{
			PageSize: 100, PageToken: pageToken, IncludeArtifacts: true,
		})
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, errors.New("A2A tasks/list returned an empty response")
		}
		for _, remote := range response.Tasks {
			mapped, err := fromA2ATask(remote)
			if err != nil {
				return nil, err
			}
			result = append(result, mapped)
		}
		pageToken = response.NextPageToken
		if pageToken == "" {
			return result, nil
		}
		if _, duplicate := seenTokens[pageToken]; duplicate {
			return nil, errors.New("A2A tasks/list repeated its page token")
		}
		seenTokens[pageToken] = struct{}{}
	}
	return nil, errors.New("A2A tasks/list exceeded pagination safety limit")
}

func (authority *A2AAuthority) Reply(
	ctx context.Context,
	taskID string,
	messageText string,
	idempotencyKey string,
) (Task, error) {
	taskID = strings.TrimSpace(taskID)
	messageID := strings.TrimSpace(idempotencyKey)
	if taskID == "" {
		return Task{}, errors.New("task id is required")
	}
	if messageID == "" {
		return Task{}, errors.New("idempotency key is required")
	}
	current, err := authority.GetTask(ctx, taskID)
	if err != nil {
		return Task{}, err
	}
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(strings.TrimSpace(messageText)))
	message.ID = messageID
	message.TaskID = a2a.TaskID(taskID)
	if contextID, ok := stringMetadata(current.Metadata, "contextId"); ok {
		message.ContextID = contextID
	}
	result, err := authority.client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: message,
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
		Metadata: map[string]any{delegation.RequestMetadataKey: map[string]any{
			"intent": "message", "idempotencyKey": messageID,
		}},
	})
	if err != nil {
		return Task{}, err
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		return Task{}, fmt.Errorf("A2A reply returned %T instead of a task", result)
	}
	return fromExpectedA2ATask(task, taskID, "reply")
}

func (authority *A2AAuthority) Cancel(
	ctx context.Context,
	taskID string,
	reason string,
	idempotencyKey string,
) (Task, error) {
	taskID = strings.TrimSpace(taskID)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if taskID == "" {
		return Task{}, errors.New("task id is required")
	}
	if idempotencyKey == "" {
		return Task{}, errors.New("idempotency key is required")
	}
	current, err := authority.GetTask(ctx, taskID)
	if err != nil {
		return Task{}, err
	}
	if current.State == StateCanceled {
		return current, nil
	}
	if terminalState(current.State) {
		return Task{}, fmt.Errorf("task %s is already terminal in state %s", taskID, current.State)
	}
	remote, err := authority.client.CancelTask(ctx, &a2a.CancelTaskRequest{
		ID: a2a.TaskID(taskID),
		Metadata: map[string]any{delegation.RequestMetadataKey: map[string]any{
			"reason": strings.TrimSpace(reason), "idempotencyKey": idempotencyKey,
		}},
	})
	if err != nil {
		// A response can be lost after the authority commits. Reconcile once so
		// retrying human_cancel is idempotent at the local tool boundary.
		reconciled, getErr := authority.GetTask(ctx, taskID)
		if getErr == nil && reconciled.State == StateCanceled {
			return reconciled, nil
		}
		return Task{}, err
	}
	return fromExpectedA2ATask(remote, taskID, "cancel")
}

func (authority *A2AAuthority) ResolveExec(
	ctx context.Context,
	taskID string,
	input ExecResolutionInput,
) (Task, error) {
	taskID = strings.TrimSpace(taskID)
	input.RequestID = strings.TrimSpace(input.RequestID)
	input.ResolutionID = strings.TrimSpace(input.ResolutionID)
	if taskID == "" || input.RequestID == "" || input.ResolutionID == "" {
		return Task{}, errors.New("task, exec request, and resolution ids are required")
	}
	if len(input.RequestID) > 128 || len(input.ResolutionID) > 128 {
		return Task{}, errors.New("exec request or resolution id exceeds protocol limits")
	}
	if len(input.Stdout) > 8<<20 || len(input.Stderr) > 8<<20 || len(strings.TrimSpace(input.Error)) > 64<<10 {
		return Task{}, errors.New("exec result exceeds protocol limits")
	}
	if !input.Approved && (len(input.Stdout) != 0 || len(input.Stderr) != 0 || input.TimedOut || input.Truncated) {
		return Task{}, errors.New("denied command cannot carry execution output")
	}
	current, err := authority.GetTask(ctx, taskID)
	if err != nil {
		return Task{}, err
	}
	request, ok := findExecRequest(current.ExecRequests, input.RequestID)
	if !ok {
		return Task{}, fmt.Errorf("exec request %q is not present on task %q", input.RequestID, taskID)
	}
	if request.Status != ExecPending {
		if execResolutionMatches(request, input) {
			return current, nil
		}
		return Task{}, fmt.Errorf("exec request %q already has a different resolution", input.RequestID)
	}
	contextID, ok := stringMetadata(current.Metadata, "contextId")
	if !ok {
		return Task{}, fmt.Errorf("task %q has no A2A context id", taskID)
	}

	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("command result"))
	message.ID = input.ResolutionID
	message.TaskID = a2a.TaskID(taskID)
	message.ContextID = contextID
	result, err := authority.client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: message,
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
		Metadata: map[string]any{delegation.RequestMetadataKey: map[string]any{
			"intent":       "command_result",
			"requestId":    input.RequestID,
			"resolutionId": input.ResolutionID,
			"approved":     input.Approved,
			"exitCode":     input.ExitCode,
			"stdoutBase64": base64.StdEncoding.EncodeToString(input.Stdout),
			"stderrBase64": base64.StdEncoding.EncodeToString(input.Stderr),
			"error":        input.Error,
			"truncated":    input.Truncated,
			"timedOut":     input.TimedOut,
		}},
	})
	if err != nil {
		// The authority may have committed before the response was lost. A
		// single owned read turns that case into the same stable replay result.
		reconciled, getErr := authority.GetTask(ctx, taskID)
		if getErr == nil {
			if request, ok := findExecRequest(reconciled.ExecRequests, input.RequestID); ok &&
				execResolutionMatches(request, input) {
				return reconciled, nil
			}
		}
		return Task{}, err
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		return Task{}, fmt.Errorf("A2A command result returned %T instead of a task", result)
	}
	return fromExpectedA2ATask(task, taskID, "resolve command")
}

func fromA2ATask(remote *a2a.Task) (Task, error) {
	if remote == nil || remote.ID == "" {
		return Task{}, errors.New("A2A response has no task id")
	}
	authorityMeta, err := decodeAuthorityMetadata(remote.Metadata)
	if err != nil {
		return Task{}, err
	}
	state, err := fromA2AState(remote.Status.State, authorityMeta.State)
	if err != nil {
		return Task{}, err
	}
	task := Task{
		ID: string(remote.ID), State: state, Revision: authorityMeta.Revision,
		LatestTurn: int(authorityMeta.LatestTurn), Message: taskMessage(remote),
		Metadata: cloneMetadata(remote.Metadata),
	}
	if task.Metadata == nil {
		task.Metadata = make(map[string]any)
	}
	task.Metadata["contextId"] = remote.ContextID
	seenExecRequests := make(map[string]struct{}, len(authorityMeta.ExecRequests))
	seenExecResolutions := make(map[string]struct{}, len(authorityMeta.ExecRequests))
	var previousExecSequence int64
	for _, request := range authorityMeta.ExecRequests {
		if request.RequestSequence <= previousExecSequence {
			return Task{}, errors.New("A2A task has unordered exec request authority metadata")
		}
		previousExecSequence = request.RequestSequence
		mapped, err := fromA2AExecRequest(task.ID, task.Revision, request)
		if err != nil {
			return Task{}, err
		}
		if _, duplicate := seenExecRequests[mapped.ID]; duplicate {
			return Task{}, fmt.Errorf("A2A task has duplicate exec request %q", mapped.ID)
		}
		seenExecRequests[mapped.ID] = struct{}{}
		if mapped.ResolutionID != "" {
			if _, duplicate := seenExecResolutions[mapped.ResolutionID]; duplicate {
				return Task{}, fmt.Errorf("A2A task has duplicate exec resolution %q", mapped.ResolutionID)
			}
			seenExecResolutions[mapped.ResolutionID] = struct{}{}
		}
		task.ExecRequests = append(task.ExecRequests, mapped)
	}
	if len(remote.Artifacts) > 0 {
		if len(remote.Artifacts) != 1 {
			return Task{}, errors.New("A2A task returned more than one live replace artifact")
		}
		artifact, err := fromA2AArtifact(task.ID, remote.Artifacts[len(remote.Artifacts)-1])
		if err != nil {
			return Task{}, err
		}
		if artifact.Turn != task.LatestTurn {
			return Task{}, fmt.Errorf("A2A artifact turn %d differs from authoritative latest turn %d", artifact.Turn, task.LatestTurn)
		}
		task.Artifact = &artifact
	} else if task.LatestTurn > 0 {
		return Task{}, errors.New("A2A task omitted its authoritative latest artifact")
	}
	return task, nil
}

type authorityWireMetadata struct {
	Revision     int64                    `json:"revision"`
	LatestTurn   int64                    `json:"latestTurn"`
	State        string                   `json:"state"`
	ExecRequests []delegation.ExecRequest `json:"execRequests"`
}

func decodeAuthorityMetadata(metadata map[string]any) (authorityWireMetadata, error) {
	value, ok := metadata[delegation.MetadataKey]
	if !ok {
		return authorityWireMetadata{}, errors.New("A2A task lacks authority metadata")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return authorityWireMetadata{}, err
	}
	var decoded authorityWireMetadata
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded.Revision < 1 || decoded.LatestTurn < 0 {
		return authorityWireMetadata{}, errors.New("A2A task has invalid authority metadata")
	}
	return decoded, nil
}

func fromA2AExecRequest(taskID string, taskRevision int64, remote delegation.ExecRequest) (ExecRequest, error) {
	remote.TaskID = strings.TrimSpace(remote.TaskID)
	remote.ID = strings.TrimSpace(remote.ID)
	remote.WorkerID = strings.TrimSpace(remote.WorkerID)
	remote.CWD = strings.TrimSpace(remote.CWD)
	remote.Reason = strings.TrimSpace(remote.Reason)
	remote.ResolutionID = strings.TrimSpace(remote.ResolutionID)
	remote.Error = strings.TrimSpace(remote.Error)
	if remote.TaskID != taskID || remote.ID == "" || remote.WorkerID == "" ||
		strings.TrimSpace(remote.Command) == "" || remote.Reason == "" {
		return ExecRequest{}, errors.New("A2A exec request identity does not match its task")
	}
	if len(remote.ID) > 128 || len(remote.Command) > 64<<10 || len(remote.CWD) > 4<<10 ||
		len(remote.Reason) > 8<<10 || remote.TimeoutMS < 0 || remote.TimeoutMS > 60*60*1000 ||
		len(remote.Stdout) > 8<<20 || len(remote.Stderr) > 8<<20 || len(remote.Error) > 64<<10 ||
		len(remote.ResolutionID) > 128 || strings.IndexByte(remote.Command, 0) >= 0 ||
		remote.RequestSequence < 1 || remote.RequestSequence > taskRevision || remote.CreatedAt.IsZero() ||
		(remote.ResolutionSequence != nil && *remote.ResolutionSequence > taskRevision) || !remote.Status.Valid() {
		return ExecRequest{}, fmt.Errorf("A2A exec request %q has invalid authority metadata", remote.ID)
	}
	if err := validateA2AExecState(remote); err != nil {
		return ExecRequest{}, err
	}
	var exitCode *int
	if remote.ExitCode != nil {
		value := *remote.ExitCode
		exitCode = &value
	}
	return ExecRequest{
		TaskID: remote.TaskID, ID: remote.ID, Command: remote.Command, CWD: remote.CWD,
		TimeoutMS: remote.TimeoutMS, Reason: remote.Reason, Status: ExecStatus(remote.Status),
		ExitCode:     exitCode,
		StdoutBase64: base64.StdEncoding.EncodeToString(append([]byte(nil), remote.Stdout...)),
		StderrBase64: base64.StdEncoding.EncodeToString(append([]byte(nil), remote.Stderr...)),
		Error:        remote.Error,
		Truncated:    remote.Truncated, TimedOut: remote.TimedOut, ResolutionID: remote.ResolutionID,
	}, nil
}

func validateA2AExecState(request delegation.ExecRequest) error {
	invalid := func() error {
		return fmt.Errorf("A2A exec request %q has inconsistent %s state", request.ID, request.Status)
	}
	switch request.Status {
	case delegation.ExecPending:
		if request.ExitCode != nil || len(request.Stdout) != 0 || len(request.Stderr) != 0 ||
			request.Error != "" || request.Truncated || request.TimedOut || request.ResolutionID != "" ||
			request.ResolutionSequence != nil || request.ResolvedAt != nil {
			return invalid()
		}
	case delegation.ExecCompleted:
		if request.ExitCode == nil || request.Error != "" || request.TimedOut ||
			!validA2AExecResolution(request) {
			return invalid()
		}
	case delegation.ExecFailed:
		if request.ExitCode == nil || (request.Error == "" && !request.TimedOut) ||
			!validA2AExecResolution(request) {
			return invalid()
		}
	case delegation.ExecDenied:
		if request.ExitCode != nil || len(request.Stdout) != 0 || len(request.Stderr) != 0 ||
			request.Error == "" || request.Truncated || request.TimedOut || !validA2AExecResolution(request) {
			return invalid()
		}
	default:
		return invalid()
	}
	return nil
}

func validA2AExecResolution(request delegation.ExecRequest) bool {
	return request.ResolutionID != "" && request.ResolutionSequence != nil &&
		*request.ResolutionSequence > request.RequestSequence && request.ResolvedAt != nil &&
		!request.ResolvedAt.Before(request.CreatedAt)
}

type artifactWireMetadata struct {
	Schema           string `json:"schema"`
	TaskID           string `json:"task_id"`
	Turn             int    `json:"turn"`
	BaseCommit       string `json:"base_commit"`
	Commit           string `json:"commit"`
	IncrementalPatch []byte `json:"incremental_patch"`
	Files            []File `json:"files"`
}

func fromA2AArtifact(taskID string, remote *a2a.Artifact) (Artifact, error) {
	if remote == nil || len(remote.Parts) != 1 || remote.Parts[0] == nil {
		return Artifact{}, errors.New("A2A delivery must contain exactly one raw part")
	}
	part := remote.Parts[0]
	if part.MediaType != delegation.GitPatchMediaType {
		return Artifact{}, fmt.Errorf("unsupported A2A artifact media type %q", part.MediaType)
	}
	cumulative := part.Raw()
	payload, err := json.Marshal(remote.Metadata)
	if err != nil {
		return Artifact{}, err
	}
	var metadata artifactWireMetadata
	if err := json.Unmarshal(payload, &metadata); err != nil {
		return Artifact{}, fmt.Errorf("decode A2A artifact metadata: %w", err)
	}
	if metadata.Schema != "human-agent.git-patch.v1" || metadata.TaskID != taskID ||
		metadata.Turn < 1 || !validGitObjectID(metadata.BaseCommit) || !validGitObjectID(metadata.Commit) {
		return Artifact{}, errors.New("A2A artifact metadata does not match its task")
	}
	authorityValue, ok := remote.Metadata[delegation.MetadataKey]
	if !ok {
		return Artifact{}, errors.New("A2A artifact lacks authority integrity metadata")
	}
	authorityPayload, err := json.Marshal(authorityValue)
	if err != nil {
		return Artifact{}, err
	}
	var integrity struct {
		Turn    int64  `json:"turn"`
		SHA256  string `json:"sha256"`
		Replace bool   `json:"replace"`
	}
	if err := json.Unmarshal(authorityPayload, &integrity); err != nil ||
		integrity.Turn != int64(metadata.Turn) || !integrity.Replace {
		return Artifact{}, errors.New("A2A artifact has invalid replace metadata")
	}
	digest := sha256.Sum256(cumulative)
	if !strings.EqualFold(integrity.SHA256, hex.EncodeToString(digest[:])) {
		return Artifact{}, errors.New("A2A artifact failed transport hash verification")
	}
	seenFiles := make(map[string]struct{}, len(metadata.Files))
	for _, file := range metadata.Files {
		if !validArtifactFile(file) {
			return Artifact{}, errors.New("A2A artifact has invalid file oracle metadata")
		}
		if _, duplicate := seenFiles[file.Path]; duplicate {
			return Artifact{}, errors.New("A2A artifact has duplicate file oracle metadata")
		}
		seenFiles[file.Path] = struct{}{}
	}
	artifact := Artifact{
		ID: string(remote.ID), TaskID: taskID, Turn: metadata.Turn,
		BaseCommit: metadata.BaseCommit,
		Commit:     metadata.Commit, CumulativePatch: append([]byte(nil), cumulative...),
		IncrementalPatch: append([]byte(nil), metadata.IncrementalPatch...),
		Files:            append([]File(nil), metadata.Files...),
	}
	artifact.SHA256 = artifactSHA256(&artifact)
	return artifact, nil
}

func validArtifactFile(file File) bool {
	if strings.TrimSpace(file.Path) == "" || file.Path != strings.TrimSpace(file.Path) ||
		strings.Contains(file.Path, "\\") || strings.HasPrefix(file.Path, "/") ||
		path.Clean(file.Path) != file.Path || file.Path == "." ||
		strings.EqualFold(file.Path, ".git") || strings.HasPrefix(strings.ToLower(file.Path), ".git/") {
		return false
	}
	switch file.Mode {
	case "000000":
		return file.BlobSHA == "deleted"
	case "100644", "100755":
		return validGitObjectID(file.BlobSHA)
	default:
		return false
	}
}

func validGitObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, digit := range value {
		if digit >= '0' && digit <= '9' {
			continue
		}
		if digit < 'a' || digit > 'f' {
			return false
		}
	}
	return true
}

func fromA2AState(remote a2a.TaskState, internal string) (State, error) {
	if internal == string(StateRewindPending) {
		return StateRewindPending, nil
	}
	switch remote {
	case a2a.TaskStateSubmitted:
		return StateSubmitted, nil
	case a2a.TaskStateWorking:
		return StateWorking, nil
	case a2a.TaskStateInputRequired:
		return StateInputRequired, nil
	case a2a.TaskStateCompleted:
		return StateCompleted, nil
	case a2a.TaskStateCanceled:
		return StateCanceled, nil
	case a2a.TaskStateRejected:
		return StateRejected, nil
	case a2a.TaskStateFailed:
		return StateFailed, nil
	default:
		return "", fmt.Errorf("unsupported A2A task state %q", remote)
	}
}

func taskMessage(task *a2a.Task) string {
	if task.Status.Message != nil {
		if text := messageText(task.Status.Message); text != "" {
			return text
		}
	}
	for index := len(task.History) - 1; index >= 0; index-- {
		if task.History[index] != nil && task.History[index].Role == a2a.MessageRoleAgent {
			if text := messageText(task.History[index]); text != "" {
				return text
			}
		}
	}
	return ""
}

func messageText(message *a2a.Message) string {
	var parts []string
	for _, part := range message.Parts {
		if part != nil && strings.TrimSpace(part.Text()) != "" {
			parts = append(parts, strings.TrimSpace(part.Text()))
		}
	}
	return strings.Join(parts, "\n")
}

func fromExpectedA2ATask(remote *a2a.Task, expectedID, operation string) (Task, error) {
	if remote == nil || string(remote.ID) != expectedID {
		actualID := ""
		if remote != nil {
			actualID = string(remote.ID)
		}
		return Task{}, fmt.Errorf("A2A %s returned task %q instead of requested task %q", operation, actualID, expectedID)
	}
	task, err := fromA2ATask(remote)
	if err != nil {
		return Task{}, err
	}
	return task, nil
}

func findExecRequest(requests []ExecRequest, requestID string) (ExecRequest, bool) {
	for _, request := range requests {
		if request.ID == requestID {
			return request, true
		}
	}
	return ExecRequest{}, false
}

func execResolutionMatches(request ExecRequest, input ExecResolutionInput) bool {
	if request.ResolutionID != input.ResolutionID || request.Truncated != input.Truncated ||
		request.TimedOut != input.TimedOut ||
		request.StdoutBase64 != base64.StdEncoding.EncodeToString(input.Stdout) ||
		request.StderrBase64 != base64.StdEncoding.EncodeToString(input.Stderr) {
		return false
	}
	if !input.Approved {
		if request.Status != ExecDenied || request.ExitCode != nil || request.StdoutBase64 != "" ||
			request.StderrBase64 != "" || request.Truncated || request.TimedOut {
			return false
		}
		return (strings.TrimSpace(input.Error) == "" && request.Error == "denied by caller") ||
			request.Error == strings.TrimSpace(input.Error)
	}
	if request.ExitCode == nil || *request.ExitCode != input.ExitCode || request.Error != strings.TrimSpace(input.Error) {
		return false
	}
	expectedStatus := ExecCompleted
	if strings.TrimSpace(input.Error) != "" || input.TimedOut {
		expectedStatus = ExecFailed
	}
	return request.Status == expectedStatus
}

func terminalState(state State) bool {
	switch state {
	case StateCompleted, StateCanceled, StateRejected, StateFailed:
		return true
	default:
		return false
	}
}

func stringMetadata(metadata map[string]any, key string) (string, bool) {
	value, ok := metadata[key].(string)
	return value, ok && value != ""
}

func cloneMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	payload, err := json.Marshal(source)
	if err != nil {
		return nil
	}
	var result map[string]any
	if json.Unmarshal(payload, &result) != nil {
		return nil
	}
	return result
}

func cloneHTTPClient(source *http.Client) *http.Client {
	if source == nil {
		return &http.Client{Timeout: 30 * time.Second}
	}
	cloned := *source
	return &cloned
}

type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (transport bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	cloned := request.Clone(request.Context())
	cloned.Header = request.Header.Clone()
	cloned.Header.Set("Authorization", "Bearer "+transport.token)
	return transport.base.RoundTrip(cloned)
}

func validateCardInterfaces(
	base *url.URL,
	card *a2a.AgentCard,
	allowCrossOrigin bool,
	allowInsecureHTTP bool,
) error {
	if card == nil || len(card.SupportedInterfaces) == 0 {
		return errors.New("A2A Agent Card has no supported interfaces")
	}
	for _, iface := range card.SupportedInterfaces {
		if iface == nil {
			return errors.New("A2A Agent Card has an empty interface")
		}
		endpoint, err := url.Parse(iface.URL)
		if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
			return fmt.Errorf("A2A Agent Card has invalid interface URL %q", iface.URL)
		}
		if endpoint.Scheme != "https" && endpoint.Scheme != "http" {
			return fmt.Errorf("A2A Agent Card has unsupported interface URL scheme %q", endpoint.Scheme)
		}
		if endpoint.Scheme == "http" && !loopbackHost(endpoint.Hostname()) && !allowInsecureHTTP {
			return fmt.Errorf("A2A interface %q uses plaintext HTTP outside loopback", iface.URL)
		}
		if !allowCrossOrigin && !sameOrigin(base, endpoint) {
			return fmt.Errorf("A2A interface %q is cross-origin; explicit opt-in is required", iface.URL)
		}
	}
	return nil
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

func loopbackHost(host string) bool {
	if strings.EqualFold(strings.TrimSpace(host), "localhost") {
		return true
	}
	address := net.ParseIP(strings.TrimSpace(host))
	return address != nil && address.IsLoopback()
}
