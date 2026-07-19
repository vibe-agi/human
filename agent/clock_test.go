package agent_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
)

func TestAgentRejectsUnpersistableClockAtConstruction(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
	}{
		{name: "zero", now: time.Time{}},
		{name: "outside Unix nanosecond range", now: time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, release := humantest.NewMemoryAgentStore()
			resource, err := framework.Own[agent.Store](store, release)
			if err != nil {
				t.Fatal(err)
			}
			config := agent.DefaultConfig()
			config.Store = resource
			config.Clock = func() time.Time { return test.now }
			service, err := agent.New(t.Context(), config)
			if service != nil || !errors.Is(err, agent.ErrInvalidArgument) {
				t.Fatalf("New = %#v, %v; want nil, ErrInvalidArgument", service, err)
			}
			if err := store.View(t.Context(), func(agent.StoreView) error { return nil }); !errors.Is(err, agent.ErrStoreClosed) {
				t.Fatalf("constructor failure did not release owned Store: %v", err)
			}
		})
	}
}

func TestAgentCanceledConstructionReleasesOwnedStore(t *testing.T) {
	store, release := humantest.NewMemoryAgentStore()
	resource, err := framework.Own[agent.Store](store, release)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	config := agent.DefaultConfig()
	config.Store = resource
	service, err := agent.New(ctx, config)
	if service != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("New with canceled context = %#v, %v; want nil, context.Canceled", service, err)
	}
	if err := store.View(context.Background(), func(agent.StoreView) error { return nil }); !errors.Is(err, agent.ErrStoreClosed) {
		t.Fatalf("canceled construction did not release owned Store: %v", err)
	}
}

func TestAgentClockFailureRollsBackOperation(t *testing.T) {
	store, release := humantest.NewMemoryAgentStore()
	resource, err := framework.Own[agent.Store](store, release)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	config := agent.DefaultConfig()
	config.Store = resource
	config.Clock = func() time.Time {
		if calls.Add(1) == 1 {
			return time.Date(2026, 7, 19, 12, 0, 0, 0, time.FixedZone("test", 8*60*60))
		}
		return time.Time{}
	}
	service, err := agent.New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Errorf("close Agent: %v", err)
		}
	})

	ref := agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "clock-authority", ID: "clock-workspace"},
		ID:        "clock-task",
	}
	_, err = service.CreateTask(context.Background(), agent.CreateTaskCommand{
		Meta:    agent.CommandMeta{ID: "clock-create"},
		Task:    ref,
		Context: agent.ContextRef{Authority: "clock-authority", ID: "clock-context"},
		Message: agent.MessageInput{ID: "clock-message", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("test")}}},
	})
	if !errors.Is(err, agent.ErrInvalidArgument) {
		t.Fatalf("CreateTask with failed Clock = %v, want ErrInvalidArgument", err)
	}
	if _, err := service.GetTask(t.Context(), ref); !errors.Is(err, agent.ErrNotFound) {
		t.Fatalf("failed Clock left a durable Task: %v", err)
	}
}
