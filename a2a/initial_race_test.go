package a2a

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

func TestConcurrentInitialRetryUsesFirstDurableWorkspace(t *testing.T) {
	service := openA2ATestAgent(t)
	releaseResolvers := make(chan struct{})
	var resolverCalls atomic.Int32
	handler := newRequestHandler(Config{
		Agent: service, Card: testAgentCard(),
		ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			call := resolverCalls.Add(1)
			if call == 2 {
				close(releaseResolvers)
			}
			<-releaseResolvers
			if call == 1 {
				return "workspace-a", nil
			}
			return "workspace-b", nil
		},
	}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
	request := &sdk.SendMessageRequest{Message: &sdk.Message{
		ID: "initial-race", Role: sdk.MessageRoleUser,
		Parts: sdk.ContentParts{sdk.NewTextPart("same exact message")},
	}}

	type result struct {
		task agent.Task
		err  error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func() {
			defer wait.Done()
			task, err := handler.acceptMessage(ctx, request)
			results <- result{task: task, err: err}
		}()
	}
	wait.Wait()
	close(results)
	var first *agent.Task
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent initial retry: %v", got.err)
		}
		if first == nil {
			copy := got.task
			first = &copy
			continue
		}
		if got.task.Ref != first.Ref || got.task.Context != first.Context {
			t.Fatalf("concurrent results differ: %#v and %#v", *first, got.task)
		}
	}
	if resolverCalls.Load() != 2 || first == nil {
		t.Fatalf("resolver calls = %d, first = %#v", resolverCalls.Load(), first)
	}
	page, err := service.ListAuthorityTasks(ctx, "authority-a", agent.TaskQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalSize != 1 || len(page.Items) != 1 {
		t.Fatalf("durable tasks = %#v", page)
	}
}
