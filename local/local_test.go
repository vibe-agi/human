package local

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/userdata"
)

func TestDefaultConfigScopesPublicStoresOutsideWorkspace(t *testing.T) {
	dataRoot := t.TempDir()
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	wantStore, err := userdata.WorkspacePath("local", config.HumanWorkspaceRoot, "store.db")
	if err != nil {
		t.Fatal(err)
	}
	wantState, err := userdata.WorkspacePath("local", config.HumanWorkspaceRoot, "workerkit-state.db")
	if err != nil {
		t.Fatal(err)
	}
	if config.Public.DatabasePath != wantStore || config.WebStatePath != wantState {
		t.Fatalf("default stores = %q, %q; want %q, %q",
			config.Public.DatabasePath, config.WebStatePath, wantStore, wantState)
	}
	if config.HumanWorkspaceRoot == "" || config.Public.MaxPending <= 0 ||
		config.Public.RetentionSweepInterval <= 0 {
		t.Fatalf("incomplete public defaults: %+v", config)
	}
	for label, path := range map[string]string{
		"service":   config.Public.DatabasePath,
		"state":     config.WebStatePath,
		"workspace": config.HumanWorkspaceRoot,
	} {
		if !filepath.IsAbs(path) {
			t.Fatalf("%s path = %q, want absolute", label, path)
		}
	}
}

func TestConfigCustomWorkspaceDerivesMatchingPrivatePaths(t *testing.T) {
	dataRoot := t.TempDir()
	t.Setenv("HUMAN_DATA_HOME", dataRoot)
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	resolvedConfig, err := (Config{HumanWorkspaceRoot: workspace}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	wantStore, _ := userdata.WorkspacePath("local", resolvedConfig.HumanWorkspaceRoot, "store.db")
	wantState, _ := userdata.WorkspacePath("local", resolvedConfig.HumanWorkspaceRoot, "workerkit-state.db")
	if resolvedConfig.Public.DatabasePath != wantStore || resolvedConfig.WebStatePath != wantState {
		t.Fatalf("custom workspace paths = %q, %q; want %q, %q",
			resolvedConfig.Public.DatabasePath, resolvedConfig.WebStatePath, wantStore, wantState)
	}
}

func TestOpenUsesSingleCallerCredentialAndExposesPublicCore(t *testing.T) {
	root := t.TempDir()
	private := filepath.Join(root, "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{
		Public:             PublicStackConfig{DatabasePath: filepath.Join(private, "store.db")},
		HumanWorkspaceRoot: filepath.Join(root, "mirror"),
		WebStatePath:       filepath.Join(private, "workerkit-state.db"),
		ListenAddress:      "127.0.0.1:0",
		WebListenAddress:   "127.0.0.1:0",
	}
	instance, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Service() == nil || instance.WebWorker() == nil || instance.CallerToken() == "" {
		t.Fatalf("incomplete local composition: service=%v worker=%v token=%q",
			instance.Service(), instance.WebWorker(), instance.CallerToken())
	}
	credentials := instance.Credentials()
	if !credentials.NewlyIssued || credentials.CallerToken != instance.CallerToken() {
		t.Fatalf("credentials = %+v", credentials)
	}
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}

	config.ExistingCallerToken = credentials.CallerToken
	reopened, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Credentials(); got.NewlyIssued || got.CallerToken != credentials.CallerToken {
		t.Fatalf("reused credentials = %+v", got)
	}
}

func TestOpenRejectsInvalidLifecycleAndPendingRestore(t *testing.T) {
	if _, err := Open(nil, Config{}); err == nil {
		t.Fatal("Open accepted nil context")
	}
	invalid := Config{ListenAddress: "0.0.0.0:0", WebStateDisabled: true}
	if instance, err := Open(context.Background(), invalid); err == nil {
		_ = instance.Close()
		t.Fatal("Open accepted a non-loopback listener")
	}
	databasePath := filepath.Join(t.TempDir(), "store.db")
	if err := os.WriteFile(databasePath+".restore-journal.json", []byte("pending"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := Config{Public: PublicStackConfig{DatabasePath: databasePath}, WebStateDisabled: true}
	if _, err := Open(context.Background(), config); err == nil || !strings.Contains(err.Error(), "interrupted offline restore") {
		t.Fatalf("pending restore open error = %v", err)
	}
	negative := Config{ShutdownTimeout: -time.Second}
	if _, err := negative.withDefaults(); err == nil {
		t.Fatal("negative shutdown timeout was accepted")
	}
}
