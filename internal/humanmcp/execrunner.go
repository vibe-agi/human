package humanmcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/vibe-agi/human/internal/callershim"
)

// CallerExecRunner adapts the caller shim's durable execution ledger to the
// asynchronous delegation command protocol. Request.ID is the exactly-once
// tool-call identity, so a response-loss retry returns the recorded result
// without executing the command again.
type CallerExecRunner struct {
	executor *callershim.Executor
	callerID string
}

func NewCallerExecRunner(executor *callershim.Executor, callerID string) (*CallerExecRunner, error) {
	callerID = strings.TrimSpace(callerID)
	if executor == nil || callerID == "" {
		return nil, errors.New("caller executor and stable caller id are required")
	}
	return &CallerExecRunner{executor: executor, callerID: callerID}, nil
}

func (runner *CallerExecRunner) Execute(ctx context.Context, request ExecRequest) (ExecOutcome, error) {
	if strings.TrimSpace(request.TaskID) == "" || strings.TrimSpace(request.ID) == "" {
		return ExecOutcome{}, errors.New("exec task and request ids are required")
	}
	input, err := json.Marshal(struct {
		Command   string `json:"command"`
		CWD       string `json:"cwd,omitempty"`
		TimeoutMS int64  `json:"timeout_ms,omitempty"`
	}{Command: request.Command, CWD: request.CWD, TimeoutMS: request.TimeoutMS})
	if err != nil {
		return ExecOutcome{}, err
	}
	response, err := runner.executor.Execute(ctx, callershim.ToolRequest{
		CallerID: runner.callerID, TaskID: request.TaskID, ToolCallID: request.ID,
		Name: "human_exec", Input: input,
	})
	if err != nil {
		return ExecOutcome{}, err
	}
	if message, ok := response.Content.(string); ok {
		if response.IsError {
			return ExecOutcome{ExitCode: -1, Error: message}, nil
		}
		return ExecOutcome{}, errors.New("caller command returned an invalid text result")
	}
	payload, err := json.Marshal(response.Content)
	if err != nil {
		return ExecOutcome{}, fmt.Errorf("encode caller command result: %w", err)
	}
	var result struct {
		ExitCode  int    `json:"exit_code"`
		Stdout    string `json:"stdout"`
		Stderr    string `json:"stderr"`
		Truncated bool   `json:"truncated"`
		TimedOut  bool   `json:"timed_out"`
	}
	if err := json.Unmarshal(payload, &result); err != nil {
		return ExecOutcome{}, fmt.Errorf("decode caller command result: %w", err)
	}
	outcome := ExecOutcome{
		ExitCode: result.ExitCode, Stdout: []byte(result.Stdout), Stderr: []byte(result.Stderr),
		Truncated: result.Truncated, TimedOut: result.TimedOut,
	}
	if response.IsError {
		switch {
		case result.TimedOut:
			outcome.Error = "command timed out"
		case result.ExitCode != 0:
			outcome.Error = fmt.Sprintf("command exited with code %d", result.ExitCode)
		default:
			outcome.Error = "command execution failed"
		}
	}
	return outcome, nil
}
