package human_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	human "github.com/vibe-agi/human"
	"github.com/vibe-agi/human/agent"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
)

func TestNewAgentPersistsTaskLifecycle(t *testing.T) {
	ctx := context.Background()
	config := human.DefaultAgentConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "agent.db")

	service, err := human.NewAgent(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	ref := agent.TaskRef{
		Workspace: agent.WorkspaceRef{Authority: "tenant-a", ID: "workspace-a"},
		ID:        "task-a",
	}
	created, err := service.CreateTask(ctx, agent.CreateTaskCommand{
		Meta:    agent.CommandMeta{ID: "create-a"},
		Task:    ref,
		Context: agent.ContextRef{Authority: "tenant-a", ID: "conversation-a"},
		Message: agent.MessageInput{ID: "message-a", Parts: []agent.Part{{MediaType: "text/plain", Data: []byte("do the work")}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := service.AcquireLease(ctx, agent.AcquireLeaseCommand{
		ID: "lease-a", Task: ref, Worker: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	working, err := service.AcceptTask(ctx, agent.WorkerTaskCommand{
		Meta: agent.WorkerCommandMeta{ID: "accept-a", ExpectedRevision: created.Revision, Grant: lease.Grant},
		Task: ref,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := human.NewAgent(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("close recovered HumanAgent: %v", err)
		}
	})
	task, err := recovered.GetTask(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != agent.TaskWorking || task.Revision != working.Revision {
		t.Fatalf("recovered task = %#v, want working revision %d", task, working.Revision)
	}
}

func TestAgentFacadeKeepsDomainTypeAndDefaults(t *testing.T) {
	var rootConfig human.AgentConfig = agent.DefaultConfig()
	var domainConfig agent.Config = human.DefaultAgentConfig()
	if !reflect.DeepEqual(rootConfig, domainConfig) {
		t.Fatalf("root Agent defaults differ from domain defaults")
	}
	var rootAgent *human.Agent
	var domainAgent *agent.Agent = rootAgent
	rootAgent = domainAgent
	_ = rootAgent
}

func TestAgentAndLLMRequireSeparateDatabaseIdentities(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "human.db")
	agentConfig := human.DefaultAgentConfig()
	agentConfig.DatabasePath = path
	service, err := human.NewAgent(ctx, agentConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	llmConfig := human.DefaultLLMConfig()
	store, openErr := llmsqlite.Open(ctx, llmsqlite.Config{Path: path})
	if openErr != nil {
		// The official adapter normally rejects the foreign schema while opening.
		return
	}
	// A custom Store may defer schema validation until the core begins recovery;
	// that constructor boundary must reject the same mixed identity as well.
	llmConfig.DeploymentID = "separate-schema-test"
	llmConfig.Store = store
	if llm, err := human.NewLLM(ctx, llmConfig); err == nil {
		_ = llm.Shutdown(ctx)
		t.Fatal("NewLLM accepted a HumanAgent database")
	}
}
