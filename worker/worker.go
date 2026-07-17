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
	GatewayURL        string
	Token             string
	MirrorRoot        string
	OutboxPath        string
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
// and one Bubble Tea model. A Worker supports one active Tea program at a time.
type Worker struct {
	client    *workerclient.Client
	state     *workerstate.Store
	model     tea.Model
	runMu     sync.Mutex
	running   bool
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

	var stateStore *workerstate.Store
	var err error
	if !config.DisableState {
		stateStore, err = workerstate.Open(ctx, config.StatePath)
		if err != nil {
			return nil, fmt.Errorf("open worker state: %w", err)
		}
	}
	client, err := workerclient.DialWithOutbox(ctx, config.GatewayURL, config.Token, config.OutboxPath)
	if err != nil {
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
		client: client,
		state:  stateStore,
		model:  tui.New(client, options...),
	}, nil
}

// Model returns the Bubble Tea model for embedding in a larger terminal app.
func (worker *Worker) Model() tea.Model {
	if worker == nil {
		return nil
	}
	worker.runMu.Lock()
	defer worker.runMu.Unlock()
	return worker.model
}

// Run starts the stock Bubble Tea program and returns its final model.
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
	if worker.running {
		worker.runMu.Unlock()
		return nil, errors.New("run worker: a Bubble Tea program is already active")
	}
	worker.running = true
	model := worker.model
	worker.runMu.Unlock()
	defer func() {
		worker.runMu.Lock()
		worker.running = false
		worker.runMu.Unlock()
	}()
	options = append([]tea.ProgramOption{tea.WithContext(ctx)}, options...)
	final, err := tea.NewProgram(model, options...).Run()
	if final != nil {
		worker.runMu.Lock()
		worker.model = final
		worker.runMu.Unlock()
	}
	return final, err
}

// Close stops network recovery and closes worker-local durable stores.
func (worker *Worker) Close() error {
	if worker == nil {
		return nil
	}
	worker.closeOnce.Do(func() {
		if worker.client != nil {
			worker.closeErr = worker.client.Close()
		}
		if worker.state != nil {
			if err := worker.state.Close(); worker.closeErr == nil {
				worker.closeErr = err
			}
		}
	})
	return worker.closeErr
}
