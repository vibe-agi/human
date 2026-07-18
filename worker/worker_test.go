package worker

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/userdata"
)

func TestDefaultConfigUsesOSUserDataForPrivateStores(t *testing.T) {
	dataRoot := t.TempDir()
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	wantOutbox, err := userdata.Path("worker", "worker-outbox.db")
	if err != nil {
		t.Fatal(err)
	}
	wantState, err := userdata.Path("worker", "worker-state.db")
	if err != nil {
		t.Fatal(err)
	}
	if config.OutboxPath != wantOutbox || config.StatePath != wantState {
		t.Fatalf("private defaults = %q / %q, want %q / %q", config.OutboxPath, config.StatePath, wantOutbox, wantState)
	}
}

func TestOpenValidatesPublicBoundary(t *testing.T) {
	t.Parallel()
	base := Config{
		GatewayURL:   DefaultWorkerEndpoint,
		Token:        "worker-token",
		MirrorRoot:   t.TempDir(),
		OutboxPath:   filepath.Join(t.TempDir(), "outbox.db"),
		DisableState: true,
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing context", mutate: func(*Config) {}},
		{name: "missing gateway", mutate: func(config *Config) { config.GatewayURL = "" }},
		{name: "missing token", mutate: func(config *Config) { config.Token = "" }},
		{name: "relative mirror", mutate: func(config *Config) { config.MirrorRoot = "mirror" }},
		{name: "relative outbox", mutate: func(config *Config) { config.OutboxPath = "outbox.db" }},
		{name: "state required", mutate: func(config *Config) { config.DisableState = false; config.StatePath = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.mutate(&config)
			ctx := context.Background()
			if test.name == "missing context" {
				ctx = nil
			}
			if opened, err := Open(ctx, config); err == nil {
				_ = opened.Close()
				t.Fatal("Open unexpectedly succeeded")
			}
		})
	}
}

func TestDefaultConfigUsesAbsolutePrivatePaths(t *testing.T) {
	t.Parallel()
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	for label, path := range map[string]string{
		"mirror": config.MirrorRoot,
		"outbox": config.OutboxPath,
		"state":  config.StatePath,
	} {
		if !filepath.IsAbs(path) {
			t.Fatalf("%s path = %q, want absolute", label, path)
		}
	}
}

func TestRunGuardRejectsConcurrentProgram(t *testing.T) {
	t.Parallel()
	opened := &Worker{model: inertModel{}, running: true}
	if _, err := opened.Run(context.Background()); err == nil {
		t.Fatal("Run accepted a second active Bubble Tea program")
	}
}

func TestRunIsSingleUseAfterProgramReturns(t *testing.T) {
	t.Parallel()
	opened := &Worker{model: quitModel{}}
	if _, err := opened.Run(
		context.Background(), tea.WithInput(nil), tea.WithOutput(io.Discard),
	); err != nil {
		t.Fatalf("first Run failed: %v", err)
	}
	if _, err := opened.Run(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "single-use") {
		t.Fatalf("second Run error = %v, want explicit single-use contract", err)
	}
}

func TestCloseCancelsAndWaitsForActiveRun(t *testing.T) {
	t.Parallel()
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	started := make(chan struct{})
	opened := &Worker{
		model:     runUntilCanceledModel{started: started},
		lifecycle: lifecycle,
		cancel:    cancelLifecycle,
	}
	runResult := make(chan error, 1)
	go func() {
		_, err := opened.Run(
			context.Background(), tea.WithInput(nil), tea.WithOutput(io.Discard),
		)
		runResult <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Worker.Run did not start")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- opened.Close() }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("close worker: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Worker.Close did not cancel and wait for Run")
	}
	select {
	case <-runResult:
	default:
		t.Fatal("Worker.Close returned before Run")
	}
	if opened.Model() != nil {
		t.Fatal("closed Worker still exposed its model")
	}
	if _, err := opened.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Run after Close error = %v, want closed lifecycle error", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

type inertModel struct{}

func (inertModel) Init() tea.Cmd                       { return nil }
func (inertModel) Update(tea.Msg) (tea.Model, tea.Cmd) { return inertModel{}, nil }
func (inertModel) View() tea.View                      { return tea.NewView("") }

type quitModel struct{}

func (quitModel) Init() tea.Cmd                       { return tea.Quit }
func (quitModel) Update(tea.Msg) (tea.Model, tea.Cmd) { return quitModel{}, nil }
func (quitModel) View() tea.View                      { return tea.NewView("") }

type runUntilCanceledModel struct {
	started chan struct{}
}

func (model runUntilCanceledModel) Init() tea.Cmd {
	close(model.started)
	return nil
}
func (model runUntilCanceledModel) Update(tea.Msg) (tea.Model, tea.Cmd) { return model, nil }
func (model runUntilCanceledModel) View() tea.View                      { return tea.NewView("") }
