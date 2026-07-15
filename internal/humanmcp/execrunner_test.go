package humanmcp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/callershim"
)

func TestCallerExecRunnerPersistsFailureAndDoesNotRerun(t *testing.T) {
	workspace := t.TempDir()
	root, err := callerfs.OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := callershim.OpenSQLiteLedger(context.Background(), filepath.Join(t.TempDir(), "exec.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	executor, err := callershim.NewExecutor(callershim.ExecutorConfig{
		Root: root, Ledger: ledger, ExecEnabled: true,
		DefaultTimeout: time.Second, MaxTimeout: 2 * time.Second, MaxOutputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewCallerExecRunner(executor, "caller-1")
	if err != nil {
		t.Fatal(err)
	}
	request := ExecRequest{
		TaskID: "task-1", ID: "exec-1", Status: ExecPending,
		Command: "printf 'once\\n' >> executions.txt; printf stdout; printf stderr >&2; exit 7",
		Reason:  "exercise durable command result",
	}
	first, err := runner.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ExitCode != 7 || string(first.Stdout) != "stdout" || string(first.Stderr) != "stderr" || first.Error == "" {
		t.Fatalf("first outcome = %+v", first)
	}
	replay, err := runner.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.ExitCode != first.ExitCode || string(replay.Stdout) != string(first.Stdout) || replay.Error != first.Error {
		t.Fatalf("replay = %+v; first = %+v", replay, first)
	}
	content, err := os.ReadFile(filepath.Join(workspace, "executions.txt"))
	if err != nil || string(content) != "once\n" {
		t.Fatalf("execution side effect = %q, %v", content, err)
	}
}
