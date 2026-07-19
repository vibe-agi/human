package human_test

import (
	"context"
	"path/filepath"
	"testing"

	human "github.com/vibe-agi/human"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
)

func TestLLMFacadeIsTransportNeutralCore(t *testing.T) {
	var rootConfig human.LLMConfig
	var domainConfig llm.Config = rootConfig
	rootConfig = domainConfig
	_ = rootConfig

	var rootService *human.LLM
	var domainService *llm.Service = rootService
	rootService = domainService
	_ = rootService

	var _ framework.Runtime = (*human.LLM)(nil)
	var _ llm.CallerEndpoint = (*human.LLM)(nil)
	var _ llm.WorkerEndpoint = (*human.LLM)(nil)
}

func TestDefaultLLMConfigRegistersFreshBuiltInCodecs(t *testing.T) {
	first := human.DefaultLLMConfig()
	second := human.DefaultLLMConfig()
	if first.DeploymentID != "" || second.DeploymentID != "" {
		t.Fatal("default config selected a deployment identity")
	}
	if _, err := first.Store.Value(); err != nil {
		t.Fatalf("inspect zero Store resource: %v", err)
	}
	if len(first.Codecs) != 3 || len(second.Codecs) != 3 {
		t.Fatalf("built-in codec count = %d, %d; want 3", len(first.Codecs), len(second.Codecs))
	}
	want := []llm.CodecID{"openai.chat", "openai.responses", "anthropic.messages"}
	for index := range want {
		if got := first.Codecs[index].Codec.Description().ID; got != want[index] {
			t.Fatalf("codec[%d] = %q, want %q", index, got, want[index])
		}
		if first.Codecs[index].Codec == second.Codecs[index].Codec {
			t.Fatalf("codec[%d] was shared between default configs", index)
		}
	}
	first.Codecs[0] = llm.CodecRegistration{}
	if second.Codecs[0].Codec == nil {
		t.Fatal("mutating one default config changed another")
	}
}

func TestNewLLMRecoversThroughOfficialSQLiteAdapter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "llm.db")
	open := func() *human.LLM {
		t.Helper()
		store, err := llmsqlite.Open(ctx, llmsqlite.Config{Path: path})
		if err != nil {
			t.Fatal(err)
		}
		config := human.DefaultLLMConfig()
		config.DeploymentID = "facade-test"
		config.Store = store
		service, err := human.NewLLM(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		return service
	}

	first := open()
	if err := first.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	second := open()
	if err := second.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Shutdown(ctx); err != nil {
		t.Fatalf("idempotent shutdown: %v", err)
	}
}
