package callershim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/safety"
)

type ToolRequest struct {
	CallerID   string          `json:"caller_id"`
	TaskID     string          `json:"task_id"`
	ToolCallID string          `json:"tool_call_id"`
	Name       string          `json:"name"`
	Input      json.RawMessage `json:"input"`
}

type ToolResponse struct {
	Content   any    `json:"content"`
	IsError   bool   `json:"is_error"`
	ErrorCode string `json:"error_code,omitempty"`
}

type ExecutorConfig struct {
	Root           *callerfs.Root
	Ledger         Ledger
	ExecEnabled    bool
	DefaultTimeout time.Duration
	MaxTimeout     time.Duration
	MaxOutputBytes int
}

type Executor struct {
	config ExecutorConfig
}

func NewExecutor(config ExecutorConfig) (*Executor, error) {
	if config.Root == nil || config.Ledger == nil {
		return nil, errors.New("caller root and durable ledger are required")
	}
	if config.DefaultTimeout <= 0 {
		config.DefaultTimeout = 30 * time.Second
	}
	if config.MaxTimeout <= 0 {
		config.MaxTimeout = 5 * time.Minute
	}
	if config.DefaultTimeout > config.MaxTimeout {
		return nil, errors.New("default exec timeout exceeds maximum")
	}
	if config.MaxOutputBytes <= 0 {
		config.MaxOutputBytes = 1 << 20
	}
	return &Executor{config: config}, nil
}

func (executor *Executor) Execute(ctx context.Context, request ToolRequest) (ToolResponse, error) {
	if request.CallerID == "" || request.TaskID == "" || request.ToolCallID == "" || request.Name == "" {
		return ToolResponse{}, errors.New("caller, task, tool call, and tool name are required")
	}
	digest, canonicalInput, err := requestDigest(request.Name, request.Input)
	if err != nil {
		return ToolResponse{}, err
	}
	key := ExecutionKey{CallerID: request.CallerID, TaskID: request.TaskID, ToolCallID: request.ToolCallID}
	begin, err := executor.config.Ledger.Begin(ctx, key, digest)
	if err != nil {
		return ToolResponse{}, err
	}
	if begin.Replay {
		if begin.Execution.Status != "completed" {
			return ToolResponse{}, ErrExecutionPending
		}
		var response ToolResponse
		if err := json.Unmarshal(begin.Execution.Response, &response); err != nil {
			return ToolResponse{}, fmt.Errorf("decode persisted tool response: %w", err)
		}
		return response, nil
	}

	response := executor.executeOnce(ctx, request.Name, canonicalInput)
	encoded, err := json.Marshal(response)
	if err != nil {
		return ToolResponse{}, fmt.Errorf("encode tool response: %w", err)
	}
	if _, err := executor.config.Ledger.Complete(ctx, key, encoded); err != nil {
		return ToolResponse{}, fmt.Errorf("persist tool response: %w", err)
	}
	return response, nil
}

func (executor *Executor) executeOnce(ctx context.Context, name string, input json.RawMessage) ToolResponse {
	var content any
	var err error
	switch name {
	case "human_read_file":
		content, err = executor.read(input)
	case "human_search":
		content, err = executor.search(input)
	case "human_write_file":
		content, err = executor.write(input)
	case "human_edit_file":
		content, err = executor.edit(input)
	case "human_delete_file":
		content, err = executor.delete(input)
	case "human_rename_file":
		content, err = executor.rename(input)
	case "human_exec":
		content, err = executor.run(ctx, input)
	default:
		err = fmt.Errorf("tool %q is not part of %s@%s", name, adapter.HumanShimID, adapter.HumanShimVersion)
	}
	if err != nil {
		var failure *commandFailure
		if errors.As(err, &failure) {
			return ToolResponse{Content: failure.result, IsError: true, ErrorCode: classifyError(err)}
		}
		return ToolResponse{Content: err.Error(), IsError: true, ErrorCode: classifyError(err)}
	}
	return ToolResponse{Content: content}
}

type pathInput struct {
	Path string `json:"path"`
}

func (executor *Executor) read(raw json.RawMessage) (any, error) {
	var input pathInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	path, err := workspaceRelative(input.Path)
	if err != nil {
		return nil, err
	}
	content, fingerprint, err := executor.config.Root.ReadFile(path)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"path": filepath.ToSlash(path), "sha256": fingerprint, "size": len(content)}
	if utf8.Valid(content) {
		result["content"] = string(content)
		result["encoding"] = "utf-8"
	} else {
		result["content"] = base64.StdEncoding.EncodeToString(content)
		result["encoding"] = "base64"
	}
	return result, nil
}

type searchInput struct {
	Query      string `json:"query"`
	Path       string `json:"path"`
	MaxResults int    `json:"max_results"`
}

func (executor *Executor) search(raw json.RawMessage) (any, error) {
	var input searchInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	path := "."
	var err error
	if strings.TrimSpace(input.Path) != "" {
		path, err = workspaceRelative(input.Path)
		if err != nil {
			return nil, err
		}
	}
	matches, err := executor.config.Root.Search(path, input.Query, input.MaxResults)
	if err != nil {
		return nil, err
	}
	return map[string]any{"matches": matches, "count": len(matches)}, nil
}

type writeInput struct {
	Path           string `json:"path"`
	Content        string `json:"content"`
	Encoding       string `json:"encoding"`
	ExpectedSHA256 string `json:"expected_sha256"`
	Mode           uint32 `json:"mode"`
}

func (executor *Executor) write(raw json.RawMessage) (any, error) {
	var input writeInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	path, err := workspaceRelative(input.Path)
	if err != nil {
		return nil, err
	}
	if input.ExpectedSHA256 == "" {
		return nil, errors.New("expected_sha256 is required")
	}
	content, err := decodeContent(input.Content, input.Encoding)
	if err != nil {
		return nil, err
	}
	fingerprint, err := executor.config.Root.WriteFileCAS(path, input.ExpectedSHA256, content, os.FileMode(input.Mode))
	if err != nil {
		return nil, err
	}
	return map[string]any{"path": filepath.ToSlash(path), "sha256": fingerprint, "size": len(content)}, nil
}

type editInput struct {
	Path           string `json:"path"`
	OldString      string `json:"old_string"`
	NewString      string `json:"new_string"`
	ExpectedSHA256 string `json:"expected_sha256"`
}

func (executor *Executor) edit(raw json.RawMessage) (any, error) {
	var input editInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	path, err := workspaceRelative(input.Path)
	if err != nil {
		return nil, err
	}
	if input.ExpectedSHA256 == "" {
		return nil, errors.New("expected_sha256 is required")
	}
	fingerprint, err := executor.config.Root.EditFileCAS(path, input.ExpectedSHA256, []byte(input.OldString), []byte(input.NewString))
	if err != nil {
		return nil, err
	}
	return map[string]any{"path": filepath.ToSlash(path), "sha256": fingerprint}, nil
}

type deleteInput struct {
	Path           string `json:"path"`
	ExpectedSHA256 string `json:"expected_sha256"`
}

func (executor *Executor) delete(raw json.RawMessage) (any, error) {
	var input deleteInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	path, err := workspaceRelative(input.Path)
	if err != nil {
		return nil, err
	}
	if input.ExpectedSHA256 == "" {
		return nil, errors.New("expected_sha256 is required")
	}
	if err := executor.config.Root.DeleteFileCAS(path, input.ExpectedSHA256); err != nil {
		return nil, err
	}
	return map[string]any{"path": filepath.ToSlash(path), "deleted": true}, nil
}

type renameInput struct {
	From           string `json:"from"`
	To             string `json:"to"`
	ExpectedSHA256 string `json:"expected_sha256"`
}

func (executor *Executor) rename(raw json.RawMessage) (any, error) {
	var input renameInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	from, err := workspaceRelative(input.From)
	if err != nil {
		return nil, err
	}
	to, err := workspaceRelative(input.To)
	if err != nil {
		return nil, err
	}
	if input.ExpectedSHA256 == "" {
		return nil, errors.New("expected_sha256 is required")
	}
	if err := executor.config.Root.RenameFileCAS(from, to, input.ExpectedSHA256); err != nil {
		return nil, err
	}
	return map[string]any{"from": filepath.ToSlash(from), "to": filepath.ToSlash(to), "sha256": input.ExpectedSHA256}, nil
}

type execInput struct {
	Command   string `json:"command"`
	CWD       string `json:"cwd"`
	TimeoutMS int64  `json:"timeout_ms"`
}

func (executor *Executor) run(ctx context.Context, raw json.RawMessage) (any, error) {
	var input execInput
	if err := decodeInput(raw, &input); err != nil {
		return nil, err
	}
	decision := safety.CheckCommand(input.Command, executor.config.ExecEnabled)
	if decision.Severity == safety.SeverityBlock {
		return nil, errors.New(strings.Join(decision.Reasons, "; "))
	}
	if strings.TrimSpace(input.Command) == "" {
		return nil, errors.New("command is required")
	}
	cwd, err := workspaceRelativeDirectory(executor.config.Root, input.CWD)
	if err != nil {
		return nil, err
	}
	timeout := executor.config.DefaultTimeout
	if input.TimeoutMS > 0 {
		timeout = time.Duration(input.TimeoutMS) * time.Millisecond
	}
	if timeout > executor.config.MaxTimeout {
		return nil, fmt.Errorf("timeout exceeds maximum %s", executor.config.MaxTimeout)
	}
	runContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var command *exec.Cmd
	if runtime.GOOS == "windows" {
		command = exec.CommandContext(runContext, "cmd.exe", "/C", input.Command)
	} else {
		command = exec.CommandContext(runContext, "/bin/sh", "-c", input.Command)
	}
	command.Dir = cwd
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "LANG=C.UTF-8", "LC_ALL=C.UTF-8"}
	stdout := &limitedBuffer{limit: executor.config.MaxOutputBytes}
	stderr := &limitedBuffer{limit: executor.config.MaxOutputBytes}
	command.Stdout = stdout
	command.Stderr = stderr
	err = command.Run()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		switch {
		case errors.As(err, &exitError):
			exitCode = exitError.ExitCode()
		case errors.Is(runContext.Err(), context.DeadlineExceeded):
			exitCode = -1
		default:
			return nil, err
		}
	}
	result := map[string]any{
		"exit_code": exitCode, "stdout": stdout.String(), "stderr": stderr.String(),
		"truncated": stdout.truncated || stderr.truncated,
	}
	if errors.Is(runContext.Err(), context.DeadlineExceeded) {
		result["timed_out"] = true
		return result, &commandFailure{result: result, exitCode: -1, message: fmt.Sprintf("command timed out after %s", timeout)}
	}
	if exitCode != 0 {
		return result, &commandFailure{result: result, exitCode: exitCode}
	}
	return result, nil
}

type commandFailure struct {
	result   map[string]any
	exitCode int
	message  string
}

func (failure *commandFailure) Error() string {
	if failure.message != "" {
		return failure.message
	}
	encoded, _ := json.Marshal(failure.result)
	return "command exited with code " + strconv.Itoa(failure.exitCode) + ": " + string(encoded)
}

type limitedBuffer struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	original := len(data)
	remaining := buffer.limit - buffer.buffer.Len()
	if remaining <= 0 {
		buffer.truncated = buffer.truncated || original > 0
		return original, nil
	}
	if len(data) > remaining {
		data = data[:remaining]
		buffer.truncated = true
	}
	_, _ = buffer.buffer.Write(data)
	return original, nil
}

func (buffer *limitedBuffer) String() string { return buffer.buffer.String() }

func requestDigest(name string, input json.RawMessage) (string, json.RawMessage, error) {
	var value any
	if len(input) == 0 {
		input = []byte(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", nil, fmt.Errorf("decode tool input: %w", err)
	}
	canonicalInput, err := json.Marshal(value)
	if err != nil {
		return "", nil, err
	}
	payload, _ := json.Marshal(struct {
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	}{Name: name, Input: canonicalInput})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), canonicalInput, nil
}

func decodeInput(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode tool input: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("tool input contains trailing data")
	}
	return nil
}

func decodeContent(content, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "utf8":
		return []byte(content), nil
	case "base64":
		return base64.StdEncoding.DecodeString(content)
	default:
		return nil, fmt.Errorf("unsupported content encoding %q", encoding)
	}
}

func workspaceRelative(value string) (string, error) {
	value = filepath.ToSlash(strings.TrimSpace(value))
	switch {
	case strings.HasPrefix(value, "/workspace/"):
		return strings.TrimPrefix(value, "/workspace/"), nil
	case value == "/workspace":
		return ".", nil
	case strings.HasPrefix(value, "/"):
		return "", callerfs.ErrOutsideRoot
	case value == "":
		return "", errors.New("path is required")
	default:
		return value, nil
	}
}

func workspaceRelativeDirectory(root *callerfs.Root, value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return root.Path(), nil
	}
	relative, err := workspaceRelative(value)
	if err != nil {
		return "", err
	}
	return root.ResolveDirectory(relative)
}

func classifyError(err error) string {
	var commandError *commandFailure
	switch {
	case errors.As(err, &commandError):
		return "command_failed"
	case errors.Is(err, callerfs.ErrOutsideRoot):
		return "outside_workspace"
	case errors.Is(err, callerfs.ErrGitWriteForbidden):
		return "git_write_forbidden"
	case errors.Is(err, callerfs.ErrPreconditionFailed):
		return "cas_mismatch"
	case errors.Is(err, callerfs.ErrEditMatch):
		return "edit_match_error"
	case errors.Is(err, callerfs.ErrDestinationExists):
		return "destination_exists"
	default:
		return "tool_error"
	}
}
