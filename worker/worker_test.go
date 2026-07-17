package worker

import (
	"context"
	"path/filepath"
	"testing"

	tea "charm.land/bubbletea/v2"
)

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

type inertModel struct{}

func (inertModel) Init() tea.Cmd                       { return nil }
func (inertModel) Update(tea.Msg) (tea.Model, tea.Cmd) { return inertModel{}, nil }
func (inertModel) View() tea.View                      { return tea.NewView("") }
