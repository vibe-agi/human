package local

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/internal/completion/adapter"
	workspacemirror "github.com/vibe-agi/human/internal/mirror"
	"github.com/vibe-agi/human/worker"
)

// TestRealOpenCodeTUIWorkspaceLoop is the product-path gate rather than a
// codec-only harness check. It deliberately crosses the real Bubble Tea input
// decoder, durable worker outbox, worker WebSocket, embedded gateway, installed
// OpenCode process, native tools, filesystem watcher, and mirror reconciliation.
// It is opt-in because it requires the exact captured OpenCode binary:
//
//	HUMAN_REAL_OPENCODE_TUI_E2E=1 go test ./local \
//	  -run TestRealOpenCodeTUIWorkspaceLoop -count=1 -v
func TestRealOpenCodeTUIWorkspaceLoop(t *testing.T) {
	if os.Getenv("HUMAN_REAL_OPENCODE_TUI_E2E") != "1" {
		t.Skip("set HUMAN_REAL_OPENCODE_TUI_E2E=1 to run the installed OpenCode CLI through the real TUI")
	}
	binary, err := exec.LookPath("opencode")
	if err != nil {
		t.Skip("opencode is not installed")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != adapter.OpenCodeVersion {
		t.Skipf("requires opencode %s; got %q (%v)", adapter.OpenCodeVersion, strings.TrimSpace(string(version)), err)
	}

	root := t.TempDir()
	callerWorkspace := filepath.Join(root, "caller-workspace")
	mirrorRoot := filepath.Join(root, "human-mirrors")
	privateRoot := filepath.Join(root, "private")
	for _, directory := range []string{callerWorkspace, mirrorRoot, privateRoot} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	const (
		workspaceKey = "opencode-tui-e2e"
		callerID     = "local-caller"
		before       = "version from the client Agent\n"
		after        = "version edited through the Human TUI\n"
	)
	if err := os.WriteFile(filepath.Join(callerWorkspace, "native.txt"), []byte(before), 0o600); err != nil {
		t.Fatal(err)
	}

	runContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	instance, err := Open(runContext, Config{
		Gateway: gateway.Config{
			DatabasePath:      filepath.Join(privateRoot, "gateway.db"),
			HeartbeatInterval: 100 * time.Millisecond,
			MaxPending:        45 * time.Second,
		},
		Worker: worker.Config{
			MirrorRoot: mirrorRoot,
			OutboxPath: filepath.Join(privateRoot, "worker-outbox.db"),
			StatePath:  filepath.Join(privateRoot, "worker-state.db"),
		},
		ListenAddress: "127.0.0.1:0",
		CallerSubject: callerID,
		WorkerSubject: "local-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Errorf("close local instance: %v", err)
		}
	})

	configHome := filepath.Join(root, "opencode-config")
	dataHome := filepath.Join(root, "opencode-data")
	for _, directory := range []string{configHome, dataHome} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeRealOpenCodeTUIConfig(t, callerWorkspace, instance, workspaceKey)

	inputReader, inputWriter := io.Pipe()
	defer inputWriter.Close()
	probe := newTUIViewProbe()
	program := tea.NewProgram(
		tuiObservedModel{inner: instance.Model(), probe: probe},
		tea.WithContext(runContext),
		tea.WithInput(inputReader),
		tea.WithOutput(io.Discard),
		tea.WithoutRenderer(),
		tea.WithoutSignals(),
	)
	programDone := make(chan error, 1)
	go func() {
		_, runErr := program.Run()
		programDone <- runErr
	}()
	program.Send(tea.WindowSizeMsg{Width: 160, Height: 48})
	t.Cleanup(func() {
		program.Quit()
		select {
		case err := <-programDone:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, tea.ErrProgramKilled) {
				t.Errorf("stop TUI program: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("TUI program did not stop")
		}
	})

	command := exec.CommandContext(runContext, binary, "run", "--pure", "--auto", "--format", "json",
		"--model", "human-e2e/human", "--dir", callerWorkspace,
		"Let the Human operator drive this turn. Execute every tool call exactly and continue until the Human ends the response.")
	command.Dir = callerWorkspace
	command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+configHome, "XDG_DATA_HOME="+dataHome)
	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, runErr := command.CombinedOutput()
		commandDone <- commandResult{output: output, err: runErr}
	}()

	// OpenCode can issue an auxiliary Chat request (for example, a title) beside
	// the Workspace request. Finish those finite requests through the same TUI,
	// then select the request whose declared native bash tool proves it is the
	// main Workspace turn.
	for attempts := 0; ; attempts++ {
		if attempts >= 4 {
			t.Fatalf("main OpenCode Workspace request never became active\n%s", probe.view())
		}
		probe.waitContains(t, runContext, "INBOX")
		_, beforeAccept := probe.snapshot()
		writeTUIInput(t, inputWriter, "a")
		probe.waitAfterContains(t, runContext, beforeAccept, "HUMAN TURN")
		if strings.Contains(probe.view(), "bash · runs on client Agent") {
			break
		}
		writeTUIInput(t, inputWriter, "OpenCode auxiliary request complete")
		probe.waitContains(t, runContext, "OpenCode auxiliary request complete")
		_, beforeAuxiliaryFinal := probe.snapshot()
		writeTUIInput(t, inputWriter, "\x04")
		probe.waitAfterAny(t, runContext, beforeAuxiliaryFinal, "INBOX", "IDLE")
	}

	writeTUIInput(t, inputWriter, "first streamed Human reply")
	probe.waitContains(t, runContext, "first streamed Human reply")
	_, beforeFirstReply := probe.snapshot()
	writeTUIInput(t, inputWriter, "\r")
	probe.waitAfterContains(t, runContext, beforeFirstReply, "stream open")
	writeTUIInput(t, inputWriter, "second streamed Human reply")
	probe.waitContains(t, runContext, "second streamed Human reply")
	_, beforeSecondReply := probe.snapshot()
	writeTUIInput(t, inputWriter, "\r")
	probe.waitAfterContains(t, runContext, beforeSecondReply, "stream open")

	// Reply -> Command, then hydrate the caller file through OpenCode's real read
	// tool. This closes one model response and auto-accepts its continuation.
	_, beforeCommandFocus := probe.snapshot()
	writeTUIInput(t, inputWriter, "\t")
	probe.waitAfterContains(t, runContext, beforeCommandFocus, "▸ Command")
	writeTUIInput(t, inputWriter, ":pull native.txt")
	probe.waitContains(t, runContext, ":pull native.txt")
	_, beforePull := probe.snapshot()
	writeTUIInput(t, inputWriter, "\r")
	probe.waitAfterContains(t, runContext, beforePull, "WAITING FOR AGENT")
	mirrorFile := filepath.Join(mirrorRoot, callerID, workspaceKey, "native.txt")
	waitForFileContent(t, runContext, mirrorFile, before)
	_, afterPullQueued := probe.snapshot()
	probe.waitAfterContains(t, runContext, afterPullQueued, "HUMAN TURN")

	// The real watcher detects this save, performs a full review, and exposes the
	// same preview/confirm interaction a Human uses in the terminal.
	if err := os.WriteFile(mirrorFile, []byte(after), 0o600); err != nil {
		t.Fatal(err)
	}
	probe.waitContains(t, runContext, "ctrl+p to preview")
	_, beforeTasksFocus := probe.snapshot()
	writeTUIInput(t, inputWriter, "\x1b[Z") // Shift+Tab: Reply -> Tasks.
	probe.waitAfterContains(t, runContext, beforeTasksFocus, "▸ Tasks")
	_, beforePreview := probe.snapshot()
	writeTUIInput(t, inputWriter, "\x10") // Ctrl+P.
	probe.waitAfterContains(t, runContext, beforePreview, "preview ready")
	_, beforeConfirm := probe.snapshot()
	writeTUIInput(t, inputWriter, "\r")
	probe.waitAfterContains(t, runContext, beforeConfirm, "waiting for client Agent result")
	waitForFileContent(t, runContext, filepath.Join(callerWorkspace, "native.txt"), after)
	_, afterEditQueued := probe.snapshot()
	probe.waitAfterContains(t, runContext, afterEditQueued, "workspace live · no unconfirmed changes")

	// Exercise the structured task editor. The resulting todowrite call crosses
	// the same outbox/WS/native-tool/result continuation path as the file edit.
	_, beforeTaskEditorFocus := probe.snapshot()
	writeTUIInput(t, inputWriter, "\x1b[Z") // Reply -> Tasks.
	probe.waitAfterContains(t, runContext, beforeTaskEditorFocus, "▸ Tasks")
	writeTUIInput(t, inputWriter, "n")
	probe.waitContains(t, runContext, "new task")
	writeTUIInput(t, inputWriter, "Verify Human TUI delivery")
	probe.waitContains(t, runContext, "Verify Human TUI delivery")
	writeTUIInput(t, inputWriter, "\r")
	probe.waitContains(t, runContext, "task draft changed")
	_, beforeTaskSync := probe.snapshot()
	writeTUIInput(t, inputWriter, "\x13") // Ctrl+S.
	probe.waitAfterContains(t, runContext, beforeTaskSync, "waiting for its next turn")
	_, afterTaskQueued := probe.snapshot()
	probe.waitAfterContains(t, runContext, afterTaskQueued, "HUMAN TURN")
	probe.waitContains(t, runContext, "Verify Human TUI delivery")
	probe.waitContains(t, runContext, "▸ Reply")

	// Reply -> Command. pwd is intentionally non-destructive, while the
	// assertion still proves the dedicated command pane reached OpenCode's bash.
	_, beforeBashFocus := probe.snapshot()
	writeTUIInput(t, inputWriter, "\t")
	probe.waitAfterContains(t, runContext, beforeBashFocus, "▸ Command")
	writeTUIInput(t, inputWriter, "pwd")
	probe.waitContains(t, runContext, "pwd")
	_, beforeBash := probe.snapshot()
	writeTUIInput(t, inputWriter, "\r")
	probe.waitAfterContains(t, runContext, beforeBash, "tool call queued")
	_, afterBashQueued := probe.snapshot()
	probe.waitAfterContains(t, runContext, afterBashQueued, "HUMAN TURN")
	probe.waitContains(t, runContext, "▸ Reply")

	// A final response is a separate gesture from streaming replies and tool
	// calls; Ctrl+D ends the Human turn and lets the real CLI terminate cleanly.
	writeTUIInput(t, inputWriter, "Human TUI workspace loop complete")
	probe.waitContains(t, runContext, "Human TUI workspace loop complete")
	writeTUIInput(t, inputWriter, "\x04")

	var result commandResult
	select {
	case result = <-commandDone:
	case <-runContext.Done():
		t.Fatalf("real OpenCode TUI loop timed out: %v\n%s", runContext.Err(), probe.view())
	}
	if result.err != nil {
		t.Fatalf("real OpenCode TUI loop failed: %v\n%s\nTUI:\n%s", result.err, result.output, probe.view())
	}
	output := string(result.output)
	for _, marker := range []string{
		"first streamed Human reply", "second streamed Human reply",
		"Human TUI workspace loop complete", "opencode debug file read --pure 'native.txt'", `"tool":"edit"`,
		`"tool":"todowrite"`, `"tool":"bash"`,
	} {
		if !strings.Contains(output, marker) && !strings.Contains(output, strings.ReplaceAll(marker, `":`, `": `)) {
			t.Fatalf("OpenCode output omitted %q:\n%s", marker, output)
		}
	}

	// Reopening the durable mirror state provides a black-box proof that the
	// native edit result was reconciled rather than merely written by OpenCode.
	mirror, err := workspacemirror.Open(mirrorRoot, callerID, workspaceKey)
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := mirror.Review()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("mirror remained dirty after real OpenCode result: %+v", remaining)
	}
}

func writeRealOpenCodeTUIConfig(t *testing.T, workspace string, instance *Local, workspaceKey string) {
	t.Helper()
	config := map[string]any{
		"$schema": "https://opencode.ai/config.json",
		"provider": map[string]any{"human-e2e": map[string]any{
			"npm": "@ai-sdk/openai-compatible", "name": "Human E2E",
			"options": map[string]any{
				"baseURL": instance.BaseURL() + "/v1", "apiKey": instance.CallerToken(),
				"headers": map[string]string{
					"X-Human-Capability-Tier": "workspace",
					"X-Human-Workspace-Key":   workspaceKey,
					"X-Human-Harness-Id":      adapter.OpenCodeID,
					"X-Human-Harness-Version": adapter.OpenCodeVersion,
					"X-Human-Workspace-Root":  workspace,
					"X-Human-Allow-Exec":      "true",
				},
			},
			"models": map[string]any{"human": map[string]any{
				"name": "Human", "limit": map[string]int{"context": 100000, "output": 4096},
			}},
		}},
	}
	payload, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "opencode.json"), append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

type tuiViewProbe struct {
	mu       sync.Mutex
	content  string
	revision uint64
	changed  chan struct{}
}

func newTUIViewProbe() *tuiViewProbe {
	return &tuiViewProbe{changed: make(chan struct{})}
}

func (probe *tuiViewProbe) record(content string) {
	probe.mu.Lock()
	probe.content = ansi.Strip(content)
	probe.revision++
	close(probe.changed)
	probe.changed = make(chan struct{})
	probe.mu.Unlock()
}

func (probe *tuiViewProbe) view() string {
	content, _ := probe.snapshot()
	return content
}

func (probe *tuiViewProbe) snapshot() (string, uint64) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	return probe.content, probe.revision
}

func (probe *tuiViewProbe) waitContains(t *testing.T, ctx context.Context, value string) {
	t.Helper()
	probe.wait(t, ctx, func(content string) bool { return strings.Contains(content, value) }, value)
}

func (probe *tuiViewProbe) waitAny(t *testing.T, ctx context.Context, values ...string) {
	t.Helper()
	probe.wait(t, ctx, func(content string) bool {
		for _, value := range values {
			if strings.Contains(content, value) {
				return true
			}
		}
		return false
	}, strings.Join(values, " or "))
}

func (probe *tuiViewProbe) waitAfterContains(
	t *testing.T,
	ctx context.Context,
	revision uint64,
	value string,
) {
	t.Helper()
	probe.waitAfter(t, ctx, revision, func(content string) bool {
		return strings.Contains(content, value)
	}, value)
}

func (probe *tuiViewProbe) waitAfterAny(
	t *testing.T,
	ctx context.Context,
	revision uint64,
	values ...string,
) {
	t.Helper()
	probe.waitAfter(t, ctx, revision, func(content string) bool {
		for _, value := range values {
			if strings.Contains(content, value) {
				return true
			}
		}
		return false
	}, strings.Join(values, " or "))
}

func (probe *tuiViewProbe) wait(t *testing.T, ctx context.Context, ready func(string) bool, want string) {
	probe.waitAfter(t, ctx, 0, ready, want)
}

func (probe *tuiViewProbe) waitAfter(
	t *testing.T,
	ctx context.Context,
	revision uint64,
	ready func(string) bool,
	want string,
) {
	t.Helper()
	for {
		probe.mu.Lock()
		content := probe.content
		currentRevision := probe.revision
		changed := probe.changed
		probe.mu.Unlock()
		if currentRevision > revision && ready(content) {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for TUI view containing %s: %v\n%s", want, ctx.Err(), content)
		}
	}
}

type tuiObservedModel struct {
	inner tea.Model
	probe *tuiViewProbe
}

func (model tuiObservedModel) Init() tea.Cmd {
	model.probe.record(model.inner.View().Content)
	return model.inner.Init()
}

func (model tuiObservedModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	next, command := model.inner.Update(message)
	wrapped := tuiObservedModel{inner: next, probe: model.probe}
	model.probe.record(next.View().Content)
	return wrapped, command
}

func (model tuiObservedModel) View() tea.View {
	view := model.inner.View()
	model.probe.record(view.Content)
	return view
}

func writeTUIInput(t *testing.T, writer io.Writer, input string) {
	t.Helper()
	if _, err := io.WriteString(writer, input); err != nil {
		t.Fatalf("write TUI input %q: %v", input, err)
	}
}

func waitForFileContent(t *testing.T, ctx context.Context, path, want string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		content, err := os.ReadFile(path)
		if err == nil && string(content) == want {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s = %q: %v (last read %q, %v)", path, want, ctx.Err(), content, err)
		}
	}
}
