// Package worker embeds a Human worker and its Bubble Tea interface in another
// Go application. Applications that only need the stock terminal program can
// use `human worker`; embedders keep control of the program lifecycle.
package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/internal/tui"
	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/internal/workerclient"
	"github.com/vibe-agi/human/internal/workerstate"
)

const DefaultWorkerEndpoint = "ws://127.0.0.1:8080/internal/v1/worker/ws"

// Config describes one worker process. Paths are deliberately explicit in the
// library API: unlike the CLI, Open does not expand shell syntax such as ~.
type Config struct {
	GatewayURL string
	Token      string
	MirrorRoot string
	OutboxPath string
	// OutboxScope is an optional stable logical gateway identity. Leave it empty
	// for normal remote deployments, where the canonical GatewayURL is stable.
	// Embedded gateways with an ephemeral listener may bind it to their durable
	// database identity so pending events survive a port change.
	OutboxScope       string
	StatePath         string
	DisableState      bool
	WorkspaceAutoSend bool
}

// DefaultConfig returns private OS user-data paths suitable for a desktop
// worker. Token remains empty and must be supplied by the embedding
// application; the editable mirror remains ~/mirror by product convention.
func DefaultConfig() (Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, fmt.Errorf("resolve worker home: %w", err)
	}
	outboxPath, err := userdata.Path("worker", "worker-outbox.db")
	if err != nil {
		return Config{}, fmt.Errorf("resolve worker outbox path: %w", err)
	}
	statePath, err := userdata.Path("worker", "worker-state.db")
	if err != nil {
		return Config{}, fmt.Errorf("resolve worker state path: %w", err)
	}
	return Config{
		GatewayURL: DefaultWorkerEndpoint,
		MirrorRoot: filepath.Join(home, "mirror"),
		OutboxPath: outboxPath,
		StatePath:  statePath,
	}, nil
}

// Worker owns the network client, durable outbox, optional TUI state store,
// and one Bubble Tea model. A Worker owns exactly one Bubble Tea program
// lifetime; open a new Worker to start another program after Run returns.
type Worker struct {
	client    *workerclient.Client
	state     *workerstate.Store
	model     tea.Model
	lifecycle context.Context
	cancel    context.CancelFunc
	runMu     sync.Mutex
	running   bool
	ran       bool
	closed    bool
	runCancel context.CancelFunc
	runDone   chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// Open connects (or starts background reconnection for a transient outage)
// and constructs the embeddable Bubble Tea model.
func Open(ctx context.Context, config Config) (*Worker, error) {
	if ctx == nil {
		return nil, errors.New("open worker: context is required")
	}
	if strings.TrimSpace(config.GatewayURL) == "" {
		return nil, errors.New("open worker: gateway URL is required")
	}
	if strings.TrimSpace(config.Token) == "" {
		return nil, errors.New("open worker: token is required")
	}
	if strings.TrimSpace(config.OutboxPath) == "" {
		return nil, errors.New("open worker: outbox path is required")
	}
	if strings.TrimSpace(config.MirrorRoot) == "" {
		return nil, errors.New("open worker: mirror root is required")
	}
	for label, path := range map[string]string{
		"outbox":      config.OutboxPath,
		"mirror root": config.MirrorRoot,
	} {
		if !filepath.IsAbs(path) && path != ":memory:" {
			return nil, fmt.Errorf("open worker: %s path must be absolute", label)
		}
	}
	if !config.DisableState && strings.TrimSpace(config.StatePath) == "" {
		return nil, errors.New("open worker: state path is required unless state persistence is disabled")
	}
	if !config.DisableState && !filepath.IsAbs(config.StatePath) && config.StatePath != ":memory:" {
		return nil, errors.New("open worker: state path must be absolute")
	}

	lifecycle, cancelLifecycle := context.WithCancel(ctx)
	var stateStore *workerstate.Store
	var err error
	if !config.DisableState {
		stateStore, err = workerstate.Open(lifecycle, config.StatePath)
		if err != nil {
			cancelLifecycle()
			return nil, fmt.Errorf("open worker state: %w", err)
		}
	}
	var client *workerclient.Client
	if strings.TrimSpace(config.OutboxScope) == "" {
		client, err = workerclient.DialWithOutbox(lifecycle, config.GatewayURL, config.Token, config.OutboxPath)
	} else {
		client, err = workerclient.DialWithOutboxScope(
			lifecycle, config.GatewayURL, config.Token, config.OutboxPath, config.OutboxScope,
		)
	}
	if err != nil {
		cancelLifecycle()
		if stateStore != nil {
			_ = stateStore.Close()
		}
		return nil, fmt.Errorf("connect worker: %w", err)
	}
	options := []tui.Option{
		tui.WithMirrorRoot(config.MirrorRoot),
		tui.WithWorkspaceAutoSend(config.WorkspaceAutoSend),
	}
	if stateStore != nil {
		options = append(options, tui.WithStateStore(stateStore))
	}
	return &Worker{
		client:    client,
		state:     stateStore,
		model:     tui.New(client, options...),
		lifecycle: lifecycle,
		cancel:    cancelLifecycle,
	}, nil
}

// Model returns the Bubble Tea model for embedding in a larger terminal app.
func (worker *Worker) Model() tea.Model {
	if worker == nil {
		return nil
	}
	worker.runMu.Lock()
	defer worker.runMu.Unlock()
	if worker.closed {
		return nil
	}
	return worker.model
}

// Run starts the stock Bubble Tea program and returns its final model. It is
// deliberately single-use: Bubble Tea commands may still be unwinding when a
// forced program exit returns, so reusing the same model would let callbacks
// from the old program mutate the new one. Open a new Worker for another run.
func (worker *Worker) Run(ctx context.Context, options ...tea.ProgramOption) (tea.Model, error) {
	if worker == nil {
		return nil, errors.New("run worker: worker is not open")
	}
	if ctx == nil {
		return nil, errors.New("run worker: context is required")
	}
	worker.runMu.Lock()
	if worker.model == nil {
		worker.runMu.Unlock()
		return nil, errors.New("run worker: worker is not open")
	}
	if worker.closed {
		worker.runMu.Unlock()
		return nil, errors.New("run worker: worker is closed")
	}
	if worker.running {
		worker.runMu.Unlock()
		return nil, errors.New("run worker: a Bubble Tea program is already active")
	}
	if worker.ran {
		worker.runMu.Unlock()
		return nil, errors.New("run worker: Worker.Run is single-use; open a new Worker")
	}
	worker.ran = true
	worker.running = true
	model := worker.model
	runContext, cancelRun := context.WithCancel(ctx)
	lifecycle := worker.lifecycle
	if lifecycle == nil {
		lifecycle = context.Background()
	}
	stopLifecycle := context.AfterFunc(lifecycle, cancelRun)
	runDone := make(chan struct{})
	worker.runCancel = cancelRun
	worker.runDone = runDone
	worker.runMu.Unlock()
	defer func() {
		stopLifecycle()
		cancelRun()
		worker.runMu.Lock()
		worker.running = false
		worker.runCancel = nil
		close(runDone)
		worker.runDone = nil
		worker.runMu.Unlock()
	}()
	options = append([]tea.ProgramOption{tea.WithContext(runContext)}, options...)
	final, err := tea.NewProgram(model, options...).Run()
	if final != nil {
		worker.runMu.Lock()
		worker.model = final
		worker.runMu.Unlock()
	}
	return final, err
}

// Close cancels and waits for an active Run, then stops network recovery and
// closes worker-local durable stores. It is safe to call more than once.
func (worker *Worker) Close() error {
	if worker == nil {
		return nil
	}
	worker.closeOnce.Do(func() {
		worker.runMu.Lock()
		worker.closed = true
		if worker.cancel != nil {
			worker.cancel()
		}
		cancelRun := worker.runCancel
		runDone := worker.runDone
		if cancelRun != nil {
			cancelRun()
		}
		worker.runMu.Unlock()
		if runDone != nil {
			<-runDone
		}

		var closeErrors []error
		if worker.client != nil {
			closeErrors = append(closeErrors, worker.client.Close())
		}
		if worker.state != nil {
			closeErrors = append(closeErrors, worker.state.Close())
		}
		worker.closeErr = errors.Join(closeErrors...)
	})
	return worker.closeErr
}
