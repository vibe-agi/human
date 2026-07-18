// Command embed-agent demonstrates the durable HumanAgent domain facade.
// It deliberately supplies no HTTP/A2A transport or authentication layer;
// production adapters must derive AuthorityID from an authenticated principal.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	human "github.com/vibe-agi/human"
	"github.com/vibe-agi/human/agent"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (resultErr error) {
	ctx := context.Background()
	path := os.Getenv("HUMAN_EMBED_AGENT_DB")
	if path == "" {
		userConfig, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		path = filepath.Join(userConfig, "human", "examples", "agent.db")
	}
	config := human.DefaultAgentConfig()
	config.DatabasePath = path
	service, err := human.NewAgent(ctx, config)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, service.Close())
	}()

	ref := agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "example-host", ID: "workspace-1"},
		ID:        "task-1",
	}
	task, err := service.CreateTask(ctx, agent.CreateTaskCommand{
		Meta:    agent.CommandMeta{ID: "create-task-1"},
		Task:    ref,
		Context: agent.ContextRef{Authority: "example-host", ID: "conversation-1"},
		Message: agent.MessageInput{
			ID:    "message-1",
			Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("Review the release plan")}},
		},
	})
	if err != nil {
		return err
	}
	fmt.Printf("task=%s state=%s revision=%d\n", task.Ref.ID, task.State, task.Revision)
	return nil
}
