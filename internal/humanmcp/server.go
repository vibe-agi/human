// Package humanmcp exposes asynchronous human delegation to local agents over
// the official Model Context Protocol Go SDK. Network/A2A concerns are behind
// Authority; caller-side patch application remains local to this process.
package humanmcp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/vibe-agi/human/internal/patchapply"
)

type State string

const (
	StateSubmitted     State = "submitted"
	StateWorking       State = "working"
	StateInputRequired State = "input-required"
	StateRewindPending State = "rewind-pending"
	StateCompleted     State = "completed"
	StateCanceled      State = "canceled"
	StateRejected      State = "rejected"
	StateFailed        State = "failed"
)

type File struct {
	Path    string `json:"path"`
	BlobSHA string `json:"blob_sha"`
	Mode    string `json:"mode"`
}

type Artifact struct {
	ID               string `json:"id"`
	TaskID           string `json:"task_id"`
	Turn             int    `json:"turn"`
	SHA256           string `json:"sha256"`
	BaseCommit       string `json:"base_commit"`
	Commit           string `json:"commit"`
	CumulativePatch  []byte `json:"-"`
	IncrementalPatch []byte `json:"-"`
	Files            []File `json:"files"`
}

type Task struct {
	ID           string         `json:"id"`
	State        State          `json:"state"`
	Revision     int64          `json:"revision"`
	LatestTurn   int            `json:"latest_turn"`
	Message      string         `json:"message,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Artifact     *Artifact      `json:"artifact,omitempty"`
	ExecRequests []ExecRequest  `json:"exec_requests,omitempty"`
}

type ExecStatus string

const (
	ExecPending   ExecStatus = "pending"
	ExecCompleted ExecStatus = "completed"
	ExecDenied    ExecStatus = "denied"
	ExecFailed    ExecStatus = "failed"
)

type ExecRequest struct {
	TaskID       string     `json:"task_id"`
	ID           string     `json:"request_id"`
	Command      string     `json:"command"`
	CWD          string     `json:"cwd,omitempty"`
	TimeoutMS    int64      `json:"timeout_ms,omitempty"`
	Reason       string     `json:"reason"`
	Status       ExecStatus `json:"status"`
	ExitCode     *int       `json:"exit_code,omitempty"`
	StdoutBase64 string     `json:"stdout_base64,omitempty"`
	StderrBase64 string     `json:"stderr_base64,omitempty"`
	Error        string     `json:"error,omitempty"`
	Truncated    bool       `json:"truncated,omitempty"`
	TimedOut     bool       `json:"timed_out,omitempty"`
	ResolutionID string     `json:"resolution_id,omitempty"`
}

type ExecResolutionInput struct {
	RequestID    string
	ResolutionID string
	Approved     bool
	ExitCode     int
	Stdout       []byte
	Stderr       []byte
	Error        string
	Truncated    bool
	TimedOut     bool
}

type ExecOutcome struct {
	ExitCode  int
	Stdout    []byte
	Stderr    []byte
	Error     string
	Truncated bool
	TimedOut  bool
}

type ExecRunner interface {
	Execute(context.Context, ExecRequest) (ExecOutcome, error)
}

type DelegateInput struct {
	Prompt           string
	BaseCommit       string
	WorkspaceDigest  string
	ExecPolicy       string
	ReferenceTaskIDs []string
	IdempotencyKey   string
}

type Authority interface {
	Delegate(context.Context, DelegateInput) (Task, error)
	GetTask(context.Context, string) (Task, error)
	ListTasks(context.Context) ([]Task, error)
	Reply(context.Context, string, string, string) (Task, error)
	Cancel(context.Context, string, string, string) (Task, error)
	ResolveExec(context.Context, string, ExecResolutionInput) (Task, error)
}

type Config struct {
	Authority  Authority
	PatchApply *patchapply.Engine
	ExecRunner ExecRunner
	Version    string
}

func NewServer(config Config) (*mcp.Server, error) {
	if config.Authority == nil {
		return nil, errors.New("delegation authority is required")
	}
	if config.Version == "" {
		config.Version = "0.1.0-dev"
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "human-mcp", Version: config.Version}, nil)
	service := &service{authority: config.Authority, patchApply: config.PatchApply, execRunner: config.ExecRunner}
	mcp.AddTool(server, &mcp.Tool{
		Name: "human_delegate", Description: "Delegate a long-running task to a human agent and return immediately with a durable task id.",
	}, service.delegate)
	mcp.AddTool(server, &mcp.Tool{
		Name: "human_status", Description: "Get the authoritative state and latest cumulative delivery for a delegated task.",
	}, service.status)
	mcp.AddTool(server, &mcp.Tool{
		Name: "human_result", Description: "Fetch the latest cumulative patch and optionally apply it to this caller workspace with hash verification.",
	}, service.result)
	mcp.AddTool(server, &mcp.Tool{
		Name: "human_reply", Description: "Send steering or clarification input to a task waiting for caller input.",
	}, service.reply)
	mcp.AddTool(server, &mcp.Tool{
		Name: "human_cancel", Description: "Cancel a non-terminal delegated task.",
	}, service.cancel)
	mcp.AddTool(server, &mcp.Tool{
		Name: "human_tasks", Description: "List caller-visible delegated tasks from the authority.",
	}, service.tasks)
	// Remote execution is a total, default-off capability. When no explicitly
	// configured caller-side runner exists, the MCP surface does not advertise
	// command tools at all.
	if config.ExecRunner != nil {
		mcp.AddTool(server, &mcp.Tool{
			Name: "human_exec_pending", Description: "List human-requested commands awaiting explicit caller approval.",
		}, service.execPending)
		mcp.AddTool(server, &mcp.Tool{
			Name: "human_exec_approve", Description: "Explicitly approve and execute one pending command in the caller workspace.",
		}, service.execApprove)
		mcp.AddTool(server, &mcp.Tool{
			Name: "human_exec_deny", Description: "Deny one pending human-requested command without executing it.",
		}, service.execDeny)
	}
	return server, nil
}

type service struct {
	authority  Authority
	patchApply *patchapply.Engine
	execRunner ExecRunner
}

type delegateArgs struct {
	Prompt           string   `json:"prompt" jsonschema:"required,task description for the human"`
	BaseCommit       string   `json:"base_commit,omitempty" jsonschema:"git commit shared with the human when available"`
	WorkspaceDigest  string   `json:"workspace_digest,omitempty" jsonschema:"content fingerprint for patch-only alignment"`
	ExecPolicy       string   `json:"exec_policy,omitempty" jsonschema:"optional caller-approved remote execution policy"`
	ReferenceTaskIDs []string `json:"reference_task_ids,omitempty" jsonschema:"completed tasks whose work context should be revived"`
	IdempotencyKey   string   `json:"idempotency_key" jsonschema:"required,stable key retained across retries"`
}

type taskArgs struct {
	TaskID string `json:"task_id" jsonschema:"required,durable delegated task id"`
}

type resultArgs struct {
	TaskID         string `json:"task_id" jsonschema:"required,durable delegated task id"`
	Apply          bool   `json:"apply,omitempty" jsonschema:"apply the latest replace-semantics patch in the caller workspace"`
	ExpectedTurn   int    `json:"expected_turn,omitempty" jsonschema:"required when apply is true; CAS turn from human_status"`
	ExpectedSHA256 string `json:"expected_sha256,omitempty" jsonschema:"required when apply is true; CAS artifact digest from human_status"`
}

type replyArgs struct {
	TaskID         string `json:"task_id" jsonschema:"required,durable delegated task id"`
	Message        string `json:"message" jsonschema:"required,caller steering or clarification response"`
	IdempotencyKey string `json:"idempotency_key" jsonschema:"required,stable key retained across retries"`
}

type cancelArgs struct {
	TaskID         string `json:"task_id" jsonschema:"required,durable delegated task id"`
	Reason         string `json:"reason,omitempty" jsonschema:"why the caller is canceling"`
	IdempotencyKey string `json:"idempotency_key" jsonschema:"required,stable key retained across retries"`
}

type execPendingArgs struct {
	TaskID string `json:"task_id,omitempty" jsonschema:"optional task filter"`
}

type execDecisionArgs struct {
	TaskID         string `json:"task_id" jsonschema:"required,durable delegated task id"`
	RequestID      string `json:"request_id" jsonschema:"required,pending command request id"`
	IdempotencyKey string `json:"idempotency_key" jsonschema:"required,stable decision key retained across retries"`
	Reason         string `json:"reason,omitempty" jsonschema:"denial reason"`
}

type execPendingOutput struct {
	Requests []ExecRequest `json:"requests"`
}

type taskOutput struct {
	Task Task `json:"task"`
}

type tasksOutput struct {
	Tasks []Task `json:"tasks"`
}

type resultOutput struct {
	Task     Task               `json:"task"`
	Artifact *artifactOutput    `json:"artifact,omitempty"`
	Applied  *patchapply.Result `json:"applied,omitempty"`
}

type artifactOutput struct {
	ID              string `json:"id"`
	TaskID          string `json:"task_id"`
	Turn            int    `json:"turn"`
	SHA256          string `json:"sha256"`
	BaseCommit      string `json:"base_commit"`
	Commit          string `json:"commit"`
	CumulativePatch string `json:"cumulative_patch_base64"`
	Files           []File `json:"files"`
}

func (service *service) delegate(
	ctx context.Context,
	request *mcp.CallToolRequest,
	args delegateArgs,
) (*mcp.CallToolResult, taskOutput, error) {
	if strings.TrimSpace(args.Prompt) == "" {
		return nil, taskOutput{}, errors.New("prompt is required")
	}
	idempotencyKey := strings.TrimSpace(args.IdempotencyKey)
	if idempotencyKey == "" {
		return nil, taskOutput{}, errors.New("idempotency key is required")
	}
	baseCommit := strings.TrimSpace(args.BaseCommit)
	if service.patchApply != nil && baseCommit != "" {
		if err := service.patchApply.ValidateBase(ctx, baseCommit); err != nil {
			return nil, taskOutput{}, err
		}
	}
	task, err := service.authority.Delegate(ctx, DelegateInput{
		Prompt: strings.TrimSpace(args.Prompt), BaseCommit: baseCommit,
		WorkspaceDigest: strings.TrimSpace(args.WorkspaceDigest), ExecPolicy: strings.TrimSpace(args.ExecPolicy),
		ReferenceTaskIDs: compactStrings(args.ReferenceTaskIDs), IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return nil, taskOutput{}, err
	}
	task = taskWithArtifactCAS(task)
	if service.patchApply != nil && baseCommit != "" {
		if err := service.patchApply.Bind(ctx, task.ID, baseCommit); err != nil {
			return nil, taskOutput{}, fmt.Errorf("bind delegated task to caller workspace: %w", err)
		}
	}
	notify(request, ctx, fmt.Sprintf("delegated task %s", task.ID), 1, 1)
	return nil, taskOutput{Task: task}, nil
}

func (service *service) status(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	args taskArgs,
) (*mcp.CallToolResult, taskOutput, error) {
	taskID, err := requiredTaskID(args.TaskID)
	if err != nil {
		return nil, taskOutput{}, err
	}
	task, err := service.authority.GetTask(ctx, taskID)
	if err != nil {
		return nil, taskOutput{}, err
	}
	return nil, taskOutput{Task: taskWithArtifactCAS(task)}, nil
}

func (service *service) result(
	ctx context.Context,
	request *mcp.CallToolRequest,
	args resultArgs,
) (*mcp.CallToolResult, resultOutput, error) {
	taskID, err := requiredTaskID(args.TaskID)
	if err != nil {
		return nil, resultOutput{}, err
	}
	task, err := service.authority.GetTask(ctx, taskID)
	if err != nil {
		return nil, resultOutput{}, err
	}
	task = taskWithArtifactCAS(task)
	output := resultOutput{Task: task}
	if task.Artifact != nil {
		digest := artifactSHA256(task.Artifact)
		output.Artifact = &artifactOutput{
			ID: task.Artifact.ID, TaskID: task.Artifact.TaskID, Turn: task.Artifact.Turn, SHA256: digest,
			BaseCommit: task.Artifact.BaseCommit, Commit: task.Artifact.Commit,
			CumulativePatch: base64.StdEncoding.EncodeToString(task.Artifact.CumulativePatch),
			Files:           append([]File(nil), task.Artifact.Files...),
		}
	}
	if !args.Apply {
		return nil, output, nil
	}
	if service.patchApply == nil {
		return nil, resultOutput{}, errors.New("caller-side patch application is not configured")
	}
	if task.Artifact == nil {
		return nil, resultOutput{}, errors.New("task has no live delivery to apply")
	}
	digest := artifactSHA256(task.Artifact)
	if args.ExpectedTurn < 1 || strings.TrimSpace(args.ExpectedSHA256) == "" {
		return nil, resultOutput{}, errors.New("expected_turn and expected_sha256 are required when apply is true")
	}
	if args.ExpectedTurn != task.Artifact.Turn || !strings.EqualFold(strings.TrimSpace(args.ExpectedSHA256), digest) {
		return nil, resultOutput{}, fmt.Errorf(
			"artifact CAS mismatch: latest turn=%d sha256=%s", task.Artifact.Turn, digest,
		)
	}
	notify(request, ctx, "applying cumulative patch", 0, 1)
	artifact := task.Artifact
	files := make([]patchapply.File, len(artifact.Files))
	for index, file := range artifact.Files {
		files[index] = patchapply.File{Path: file.Path, BlobSHA: file.BlobSHA, Mode: file.Mode}
	}
	applied, err := service.patchApply.Apply(ctx, patchapply.Artifact{
		TaskID: artifact.TaskID, Turn: artifact.Turn, BaseCommit: artifact.BaseCommit,
		Commit: artifact.Commit, CumulativePatch: artifact.CumulativePatch,
		IncrementalPatch: artifact.IncrementalPatch, Files: files,
	})
	if err != nil {
		return nil, resultOutput{}, err
	}
	output.Applied = &applied
	notify(request, ctx, "cumulative patch applied and hashes verified", 1, 1)
	return nil, output, nil
}

func (service *service) reply(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	args replyArgs,
) (*mcp.CallToolResult, taskOutput, error) {
	if strings.TrimSpace(args.Message) == "" {
		return nil, taskOutput{}, errors.New("reply message is required")
	}
	if strings.TrimSpace(args.IdempotencyKey) == "" {
		return nil, taskOutput{}, errors.New("idempotency key is required")
	}
	taskID, err := requiredTaskID(args.TaskID)
	if err != nil {
		return nil, taskOutput{}, err
	}
	task, err := service.authority.Reply(ctx, taskID, strings.TrimSpace(args.Message), strings.TrimSpace(args.IdempotencyKey))
	if err != nil {
		return nil, taskOutput{}, err
	}
	return nil, taskOutput{Task: taskWithArtifactCAS(task)}, nil
}

func (service *service) cancel(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	args cancelArgs,
) (*mcp.CallToolResult, taskOutput, error) {
	if strings.TrimSpace(args.IdempotencyKey) == "" {
		return nil, taskOutput{}, errors.New("idempotency key is required")
	}
	taskID, err := requiredTaskID(args.TaskID)
	if err != nil {
		return nil, taskOutput{}, err
	}
	task, err := service.authority.Cancel(ctx, taskID, strings.TrimSpace(args.Reason), strings.TrimSpace(args.IdempotencyKey))
	if err != nil {
		return nil, taskOutput{}, err
	}
	return nil, taskOutput{Task: taskWithArtifactCAS(task)}, nil
}

func artifactSHA256(artifact *Artifact) string {
	if artifact == nil {
		return ""
	}
	files := append([]File(nil), artifact.Files...)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Path == files[j].Path {
			return files[i].BlobSHA < files[j].BlobSHA
		}
		return files[i].Path < files[j].Path
	})
	payload, _ := json.Marshal(struct {
		ID               string `json:"id"`
		TaskID           string `json:"task_id"`
		Turn             int    `json:"turn"`
		BaseCommit       string `json:"base_commit"`
		Commit           string `json:"commit"`
		CumulativePatch  []byte `json:"cumulative_patch"`
		IncrementalPatch []byte `json:"incremental_patch"`
		Files            []File `json:"files"`
	}{
		ID: artifact.ID, TaskID: artifact.TaskID, Turn: artifact.Turn,
		BaseCommit: artifact.BaseCommit, Commit: artifact.Commit,
		CumulativePatch: artifact.CumulativePatch, IncrementalPatch: artifact.IncrementalPatch,
		Files: files,
	})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func taskWithArtifactCAS(task Task) Task {
	if task.Artifact == nil {
		return task
	}
	artifact := *task.Artifact
	artifact.CumulativePatch = append([]byte(nil), artifact.CumulativePatch...)
	artifact.IncrementalPatch = append([]byte(nil), artifact.IncrementalPatch...)
	artifact.Files = append([]File(nil), artifact.Files...)
	artifact.SHA256 = artifactSHA256(&artifact)
	task.Artifact = &artifact
	return task
}

func (service *service) tasks(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ struct{},
) (*mcp.CallToolResult, tasksOutput, error) {
	tasks, err := service.authority.ListTasks(ctx)
	if err != nil {
		return nil, tasksOutput{}, err
	}
	if tasks == nil {
		tasks = []Task{}
	}
	for index := range tasks {
		tasks[index] = taskWithArtifactCAS(tasks[index])
	}
	return nil, tasksOutput{Tasks: tasks}, nil
}

func (service *service) execPending(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	args execPendingArgs,
) (*mcp.CallToolResult, execPendingOutput, error) {
	var tasks []Task
	if strings.TrimSpace(args.TaskID) != "" {
		task, err := service.authority.GetTask(ctx, strings.TrimSpace(args.TaskID))
		if err != nil {
			return nil, execPendingOutput{}, err
		}
		tasks = []Task{task}
	} else {
		var err error
		tasks, err = service.authority.ListTasks(ctx)
		if err != nil {
			return nil, execPendingOutput{}, err
		}
	}
	requests := make([]ExecRequest, 0)
	for _, task := range tasks {
		for _, request := range task.ExecRequests {
			if request.Status == ExecPending {
				requests = append(requests, cloneExecRequest(request))
			}
		}
	}
	return nil, execPendingOutput{Requests: requests}, nil
}

func (service *service) execApprove(
	ctx context.Context,
	request *mcp.CallToolRequest,
	args execDecisionArgs,
) (*mcp.CallToolResult, taskOutput, error) {
	if service.execRunner == nil {
		return nil, taskOutput{}, errors.New("caller-side remote execution is disabled")
	}
	task, pending, key, replay, err := service.pendingExec(ctx, args, true)
	if err != nil {
		return nil, taskOutput{}, err
	}
	if replay {
		return nil, taskOutput{Task: taskWithArtifactCAS(task)}, nil
	}
	notify(request, ctx, "executing explicitly approved caller command", 0, 1)
	outcome, err := service.execRunner.Execute(ctx, pending)
	if err != nil {
		return nil, taskOutput{}, err
	}
	resolved, err := service.authority.ResolveExec(ctx, task.ID, ExecResolutionInput{
		RequestID: pending.ID, ResolutionID: key, Approved: true,
		ExitCode: outcome.ExitCode, Stdout: outcome.Stdout, Stderr: outcome.Stderr,
		Error: outcome.Error, Truncated: outcome.Truncated, TimedOut: outcome.TimedOut,
	})
	if err != nil {
		return nil, taskOutput{}, err
	}
	notify(request, ctx, "caller command result delivered", 1, 1)
	return nil, taskOutput{Task: taskWithArtifactCAS(resolved)}, nil
}

func (service *service) execDeny(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	args execDecisionArgs,
) (*mcp.CallToolResult, taskOutput, error) {
	task, pending, key, replay, err := service.pendingExec(ctx, args, false)
	if err != nil {
		return nil, taskOutput{}, err
	}
	if replay {
		return nil, taskOutput{Task: taskWithArtifactCAS(task)}, nil
	}
	reason := strings.TrimSpace(args.Reason)
	if reason == "" {
		reason = "denied by caller"
	}
	resolved, err := service.authority.ResolveExec(ctx, task.ID, ExecResolutionInput{
		RequestID: pending.ID, ResolutionID: key, Error: reason,
	})
	if err != nil {
		return nil, taskOutput{}, err
	}
	return nil, taskOutput{Task: taskWithArtifactCAS(resolved)}, nil
}

func (service *service) pendingExec(
	ctx context.Context,
	args execDecisionArgs,
	approved bool,
) (Task, ExecRequest, string, bool, error) {
	taskID, err := requiredTaskID(args.TaskID)
	if err != nil {
		return Task{}, ExecRequest{}, "", false, err
	}
	requestID := strings.TrimSpace(args.RequestID)
	key := strings.TrimSpace(args.IdempotencyKey)
	if requestID == "" || key == "" {
		return Task{}, ExecRequest{}, "", false, errors.New("request id and idempotency key are required")
	}
	task, err := service.authority.GetTask(ctx, taskID)
	if err != nil {
		return Task{}, ExecRequest{}, "", false, err
	}
	for _, request := range task.ExecRequests {
		if request.ID == requestID {
			if request.Status != ExecPending {
				sameDecision := approved && (request.Status == ExecCompleted || request.Status == ExecFailed)
				sameDecision = sameDecision || (!approved && request.Status == ExecDenied)
				if request.ResolutionID == key && sameDecision {
					return task, request, key, true, nil
				}
				return Task{}, ExecRequest{}, "", false, errors.New("command request is already resolved with a different decision")
			}
			return task, request, key, false, nil
		}
	}
	return Task{}, ExecRequest{}, "", false, errors.New("pending command request was not found")
}

func cloneExecRequest(request ExecRequest) ExecRequest {
	if request.ExitCode != nil {
		value := *request.ExitCode
		request.ExitCode = &value
	}
	return request
}

func requiredTaskID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("task id is required")
	}
	return value, nil
}

func compactStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func notify(request *mcp.CallToolRequest, ctx context.Context, message string, progress, total float64) {
	if request == nil || request.Session == nil || request.Params == nil {
		return
	}
	token := request.Params.GetProgressToken()
	if token == nil {
		return
	}
	_ = request.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
		ProgressToken: token, Progress: progress, Total: total, Message: message,
	})
}
