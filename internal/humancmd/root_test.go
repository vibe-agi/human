package humancmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/userdata"
)

func TestCommandExposesWorkerAndShim(t *testing.T) {
	t.Parallel()
	subcommands := New().Commands()
	var names []string
	for _, command := range subcommands {
		names = append(names, command.Name())
	}
	if strings.Join(names, ",") != "doctor,gateway,init,local,shim,version,worker" {
		t.Fatalf("subcommands = %v; want doctor, gateway, init, local, shim, version, and worker", names)
	}
}

func TestRootHasNoImplicitWorkerInvocation(t *testing.T) {
	t.Parallel()
	command := New()
	if command.RunE != nil {
		t.Fatal("bare human unexpectedly runs a worker")
	}
	for _, name := range []string{"gateway", "token", "mirror-root", "workspace-auto-send", "outbox", "state-db"} {
		if command.Flags().Lookup(name) != nil {
			t.Fatalf("removed top-level worker flag %q remains", name)
		}
	}
}

func TestEveryRunnableCommandRejectsPositionalArguments(t *testing.T) {
	t.Parallel()
	var visit func(*cobra.Command)
	visit = func(command *cobra.Command) {
		if command.Run != nil || command.RunE != nil {
			if command.Args == nil {
				t.Errorf("runnable command %q has no positional-argument gate", command.CommandPath())
			} else if err := command.Args(command, []string{"unexpected"}); err == nil {
				t.Errorf("runnable command %q accepts an unexpected positional argument", command.CommandPath())
			}
		}
		for _, child := range command.Commands() {
			visit(child)
		}
	}
	visit(New())
}

func TestMisspelledSubcommandsAndExtraArgumentsStopBeforeRun(t *testing.T) {
	testCases := []struct {
		name string
		path []string
		arg  string
	}{
		{name: "gateway misspelled subcommand", path: []string{"gateway"}, arg: "backup"},
		{name: "gateway token issue extra argument", path: []string{"gateway", "token", "issue"}, arg: "extra"},
		{name: "gateway token revoke extra argument", path: []string{"gateway", "token", "revoke"}, arg: "extra"},
		{name: "worker extra argument", path: []string{"worker"}, arg: "extra"},
		{name: "local misspelled subcommand", path: []string{"local"}, arg: "restroe"},
		{name: "local credentials extra argument", path: []string{"local", "credentials"}, arg: "extra"},
		{name: "shim extra argument", path: []string{"shim"}, arg: "extra"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			root := New()
			target, remaining, err := root.Find(testCase.path)
			if err != nil {
				t.Fatal(err)
			}
			if len(remaining) != 0 {
				t.Fatalf("find %v left arguments %v", testCase.path, remaining)
			}

			// Stand in for the service/database side effect at the very start of
			// RunE. The positional-argument gate must reject before this code can
			// be reached.
			marker := filepath.Join(t.TempDir(), "runtime-entered.db")
			ran := false
			target.Run = nil
			target.RunE = func(_ *cobra.Command, _ []string) error {
				ran = true
				return os.WriteFile(marker, []byte("entered"), 0o600)
			}

			root.SetOut(&bytes.Buffer{})
			root.SetErr(&bytes.Buffer{})
			root.SetArgs(append(append([]string(nil), testCase.path...), testCase.arg))
			err = root.ExecuteContext(context.Background())
			if err == nil || !strings.Contains(err.Error(), "unknown command") {
				t.Fatalf("unexpected argument error = %v", err)
			}
			if ran {
				t.Fatal("runnable command was entered despite an unexpected positional argument")
			}
			if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
				t.Fatalf("runtime side-effect marker exists or could not be checked: %v", statErr)
			}
		})
	}
}

func TestTopLevelHelpContainsOnlyProductCommands(t *testing.T) {
	t.Parallel()
	command := New()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--help"})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	help := output.String()
	for _, name := range []string{"doctor", "gateway", "init", "local", "shim", "version", "worker"} {
		if !strings.Contains(help, "  "+name) {
			t.Fatalf("top-level help omitted %q:\n%s", name, help)
		}
	}
	if strings.Contains(help, "  completion") {
		t.Fatalf("top-level help exposed framework completion command:\n%s", help)
	}
}

func TestWorkerSubcommandHasStandaloneFlags(t *testing.T) {
	t.Parallel()
	worker, _, err := New().Find([]string{"worker"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"gateway", "token-file", "mirror-root", "workspace-auto-send", "outbox", "state-db"} {
		if worker.Flags().Lookup(name) == nil {
			t.Fatalf("worker flag %q is missing", name)
		}
	}
	if worker.Flags().Lookup("token") != nil {
		t.Fatal("worker exposes a plaintext token argv flag")
	}
}

func TestWorkerSubcommandBindsUsableDefaults(t *testing.T) {
	withoutEnvironment(t, "HUMAN_GATEWAY_TOKEN")
	withoutEnvironment(t, "HUMAN_GATEWAY_TOKEN_FILE")
	command := New()
	worker, _, err := command.Find([]string{"worker"})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"gateway":     "ws://127.0.0.1:8080/internal/v1/worker/ws",
		"mirror-root": "~/mirror",
		"outbox":      automaticPrivatePath,
		"state-db":    automaticPrivatePath,
	} {
		flag := worker.Flags().Lookup(name)
		if flag == nil || flag.DefValue != want {
			t.Fatalf("worker %s default = %+v, want %q", name, flag, want)
		}
	}
	// The command's RunE closes over the same Viper instance bound above. A
	// missing token must therefore be the first validation error, rather than a
	// false missing-default error for gateway or private paths.
	worker.SetContext(context.Background())
	if err := worker.RunE(worker, nil); err == nil || !strings.Contains(err.Error(), "HUMAN_GATEWAY_TOKEN") {
		t.Fatalf("worker default validation error = %v", err)
	}
}

func TestLocalSubcommandRunsEmbeddedStackDirectly(t *testing.T) {
	t.Parallel()
	local, _, err := New().Find([]string{"local"})
	if err != nil {
		t.Fatal(err)
	}
	if local.RunE == nil {
		t.Fatal("local has no direct RunE")
	}
	for _, name := range []string{"listen", "db", "credentials", "workspace", "mirror-root", "workspace-auto-send", "stream-write-timeout"} {
		if local.Flag(name) == nil {
			t.Fatalf("local flag %q is missing", name)
		}
	}
}

func TestGatewaySubcommandRunsServerDirectly(t *testing.T) {
	t.Parallel()
	gateway, _, err := New().Find([]string{"gateway"})
	if err != nil {
		t.Fatal(err)
	}
	if gateway.RunE == nil {
		t.Fatal("gateway has no direct RunE")
	}
	for _, name := range []string{"listen", "db", "queue-capacity", "stream-write-timeout", "max-pending"} {
		if gateway.Flag(name) == nil {
			t.Fatalf("gateway flag %q is missing", name)
		}
	}
}

func TestWorkerMirrorRootDefaultsToDocumentedLocation(t *testing.T) {
	t.Parallel()
	command, _, err := New().Find([]string{"worker"})
	if err != nil {
		t.Fatal(err)
	}
	flag := command.Flags().Lookup("mirror-root")
	if flag == nil || flag.DefValue != "~/mirror" {
		t.Fatalf("mirror-root flag = %+v", flag)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveMirrorRoot(flag.DefValue)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != filepath.Join(home, "mirror") {
		t.Fatalf("resolved mirror root = %q, want %q", resolved, filepath.Join(home, "mirror"))
	}
}

func TestWorkerOutboxDefaultsToPrivateStateDirectory(t *testing.T) {
	t.Parallel()
	command, _, err := New().Find([]string{"worker"})
	if err != nil {
		t.Fatal(err)
	}
	flag := command.Flags().Lookup("outbox")
	if flag == nil || flag.DefValue != automaticPrivatePath {
		t.Fatalf("outbox flag = %+v", flag)
	}
	resolved, err := resolveUserDataPath(flag.DefValue, "worker outbox", "worker", "worker-outbox.db")
	if err != nil {
		t.Fatal(err)
	}
	want, err := userdata.Path("worker", "worker-outbox.db")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != want {
		t.Fatalf("resolved outbox = %q, want %q", resolved, want)
	}
}

func TestWorkerStateDBDefaultsToPrivateStateDirectory(t *testing.T) {
	t.Parallel()
	command, _, err := New().Find([]string{"worker"})
	if err != nil {
		t.Fatal(err)
	}
	flag := command.Flags().Lookup("state-db")
	if flag == nil || flag.DefValue != automaticPrivatePath {
		t.Fatalf("state-db flag = %+v", flag)
	}
	if flag.Usage == "" {
		t.Fatal("state-db flag has no usage text")
	}
	resolved, err := resolveOptionalUserDataPath(flag.DefValue, "worker state database", "worker", "worker-state.db")
	if err != nil {
		t.Fatal(err)
	}
	want, err := userdata.Path("worker", "worker-state.db")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != want {
		t.Fatalf("resolved state database = %q, want %q", resolved, want)
	}
}

func TestLocalAutomaticPrivatePathsUseRealGitWorkspaceScope(t *testing.T) {
	t.Parallel()
	project := filepath.Join(t.TempDir(), "project")
	nested := filepath.Join(project, "nested")
	if err := os.MkdirAll(filepath.Join(project, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	workspace, err := resolveLocalWorkspace(nested)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolveWorkspaceDataPath(automaticPrivatePath, "local database", "local", workspace, "gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	want, err := userdata.WorkspacePath("local", workspace, "gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("local database = %q, want %q", got, want)
	}
	if strings.HasPrefix(got, project+string(filepath.Separator)) {
		t.Fatalf("automatic local database leaked into customer workspace: %q", got)
	}
}

func TestWorkerStateDBCanBeDisabled(t *testing.T) {
	t.Parallel()
	command, _, err := New().Find([]string{"worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := command.ParseFlags([]string{"--state-db="}); err != nil {
		t.Fatal(err)
	}
	flag := command.Flags().Lookup("state-db")
	if flag == nil || !flag.Changed || flag.Value.String() != "" {
		t.Fatalf("state-db disable flag = %+v", flag)
	}
	resolved, err := resolveOptionalUserDataPath(flag.Value.String(), "worker state database", "worker", "worker-state.db")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != "" {
		t.Fatalf("disabled state database resolved to %q", resolved)
	}
}

func TestRootConfigReachesGatewayAdministration(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "configured.db")
	configPath := filepath.Join(directory, "human.toml")
	if err := os.WriteFile(configPath, []byte("db = \""+databasePath+"\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := New()
	command.SetArgs([]string{
		"--config", configPath, "gateway", "token", "issue", "--subject", "configured-caller",
	})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(databasePath); err != nil {
		t.Fatalf("configured gateway database was not created: %v", err)
	}
}

func TestConfigDoesNotAutoLoadFromUntrustedWorkingDirectory(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "human.yaml"), []byte(
		"gateway:\n  url: wss://attacker.example/worker\n  token_file: /private/secret\n",
	), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(directory)
	settings := viper.New()
	if err := loadConfig(settings); err != nil {
		t.Fatal(err)
	}
	if settings.GetString("gateway.url") != "" || settings.GetString("gateway.token_file") != "" {
		t.Fatalf("working-directory config was loaded: %+v", settings.AllSettings())
	}
}

func TestResolveOptionalPrivatePathValidatesEnabledPath(t *testing.T) {
	t.Parallel()
	if _, err := resolveOptionalUserDataPath("~someone/worker-state.db", "worker state database", "worker", "worker-state.db"); err == nil {
		t.Fatal("expected ambiguous user expansion to be rejected")
	}
}

func TestResolveMirrorRootRejectsAmbiguousUserExpansion(t *testing.T) {
	t.Parallel()
	if _, err := resolveMirrorRoot("~someone/mirror"); err == nil {
		t.Fatal("expected ~user mirror root to be rejected")
	}
}
