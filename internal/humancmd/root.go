// Package humancmd assembles the human worker TUI command.
package humancmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/gatewaycmd"
	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

const automaticPrivatePath = "auto"

func New() *cobra.Command {
	settings := viper.New()
	settings.SetEnvPrefix("HUMAN")
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	settings.AutomaticEnv()
	command := &cobra.Command{
		Use: "human", Short: "Human-as-model local runtime, gateway, and worker",
		SilenceUsage: true, SilenceErrors: true,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error { return loadConfig(settings) },
	}
	command.PersistentFlags().String("config", "", "configuration file (toml/yaml/json)")
	mustBind(settings, "config", command.PersistentFlags().Lookup("config"))
	workerCommand := &cobra.Command{
		Use: "worker", Short: "connect this Human worker to a gateway",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return run(command.Context(), settings)
		},
	}
	addWorkerFlags(workerCommand.Flags())
	for key, name := range map[string]string{
		"gateway.url": "gateway", "gateway.token_file": "token-file",
		"workspace.root":      "workspace",
		"worker.caller_scope": "caller-scope", "worker.workspace_scope": "workspace-scope",
		"worker.outbox": "outbox", "worker.state_db": "state-db", "worker.web": "web",
	} {
		mustBind(settings, key, workerCommand.Flags().Lookup(name))
	}
	command.AddCommand(workerCommand)
	command.AddCommand(newDoctorCommand())
	command.AddCommand(gatewaycmd.NewGatewayCommand())
	command.AddCommand(newInitCommand())
	command.AddCommand(newLocalCommand(settings))
	command.AddCommand(newShimCommand(settings))
	command.AddCommand(newVersionCommand())
	return command
}

func addWorkerFlags(flags *pflag.FlagSet) {
	flags.String("gateway", "ws://127.0.0.1:8080/internal/v1/worker/ws", "gateway worker WebSocket URL")
	flags.String("token-file", "", "private file containing the worker token (or set HUMAN_GATEWAY_TOKEN)")
	flags.String("workspace", "~/human-workspace", "Human-side base directory; each agent session gets a stable child working directory")
	flags.String("caller-scope", "", "authenticated Agent-user caller ID authorized for this mirror")
	flags.String("workspace-scope", "", "opaque Agent-user workspace key authorized for this mirror")
	flags.String("outbox", automaticPrivatePath, "private SQLite outbox; auto uses the OS user-data directory")
	flags.String("state-db", automaticPrivatePath, "reserved; legacy TUI state flag kept for config compatibility")
	flags.String("web", "", "loopback address of the browser human side; the printed login URL carries a per-start session token, so keep it off shared terminals and proxy logs (default 127.0.0.1:19082)")
}

func run(ctx context.Context, settings *viper.Viper) error {
	url := strings.TrimSpace(settings.GetString("gateway.url"))
	if url == "" {
		return errors.New("gateway URL is required")
	}
	token, err := resolveSecret("HUMAN_GATEWAY_TOKEN", settings.GetString("gateway.token_file"), "worker token")
	if err != nil {
		return err
	}
	mirrorRoot, err := resolveHumanWorkspace(settings.GetString("workspace.root"))
	if err != nil {
		return err
	}
	outboxPath, err := resolveUserDataPath(settings.GetString("worker.outbox"), "worker outbox", "worker", "worker-outbox.db")
	if err != nil {
		return err
	}
	statePath, err := resolveOptionalUserDataPath(settings.GetString("worker.state_db"), "worker state database", "worker", "worker-state.db")
	if err != nil {
		return err
	}
	_ = statePath
	webAddress := strings.TrimSpace(settings.GetString("worker.web"))
	if webAddress == "" {
		webAddress = "127.0.0.1:19082"
	}
	mirrorScope := workerkit.WorkspaceScope{
		Caller:       llm.CallerID(strings.TrimSpace(settings.GetString("worker.caller_scope"))),
		WorkspaceKey: strings.TrimSpace(settings.GetString("worker.workspace_scope")),
	}
	if err := mirrorScope.Validate(); err != nil {
		return fmt.Errorf("configure remote Human mirror: %w", err)
	}
	return runWebWorker(ctx, url, token, mirrorRoot, outboxPath, webAddress, mirrorScope)
}

func resolveHumanWorkspace(value string) (string, error) {
	path, err := resolvePrivatePath(value, "Human workspace")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", fmt.Errorf("create Human workspace: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve Human workspace: %w", err)
	}
	return canonical, nil
}

func resolveUserDataPath(value, description string, elements ...string) (string, error) {
	if strings.TrimSpace(value) != automaticPrivatePath {
		return resolvePrivatePath(value, description)
	}
	path, err := userdata.Path(elements...)
	if err != nil {
		return "", fmt.Errorf("resolve automatic %s: %w", description, err)
	}
	return path, nil
}

func resolveOptionalUserDataPath(value, description string, elements ...string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	return resolveUserDataPath(value, description, elements...)
}

func resolveWorkspaceDataPath(value, description, component, realWorkspace string, elements ...string) (string, error) {
	if strings.TrimSpace(value) != automaticPrivatePath {
		return resolvePrivatePath(value, description)
	}
	path, err := userdata.WorkspacePath(component, realWorkspace, elements...)
	if err != nil {
		return "", fmt.Errorf("resolve automatic %s: %w", description, err)
	}
	return path, nil
}

func resolvePrivatePath(value, description string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", description)
	}
	if value == "~" || strings.HasPrefix(value, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory for %s: %w", description, err)
		}
		if value == "~" {
			value = home
		} else {
			value = filepath.Join(home, filepath.FromSlash(strings.TrimPrefix(value, "~/")))
		}
	} else if strings.HasPrefix(value, "~") {
		return "", fmt.Errorf("%s supports only ~ or ~/path expansion", description)
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", description, err)
	}
	return absolute, nil
}

func loadConfig(settings *viper.Viper) error {
	if configFile := settings.GetString("config"); configFile != "" {
		settings.SetConfigFile(configFile)
		return settings.ReadInConfig()
	}
	// Never auto-load configuration from the current directory. Human is meant
	// to be launched inside untrusted customer workspaces; a checked-in
	// human.yaml must not be able to redirect a token file or bearer-authenticated
	// endpoint. Configuration files are therefore explicit-only.
	return nil
}

func mustBind(settings *viper.Viper, key string, flag *pflag.Flag) {
	if err := settings.BindPFlag(key, flag); err != nil {
		panic(err)
	}
}
