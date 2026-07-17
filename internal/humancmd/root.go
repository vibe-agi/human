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
	workerlib "github.com/vibe-agi/human/worker"
)

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
		RunE: func(command *cobra.Command, _ []string) error {
			return run(command.Context(), settings)
		},
	}
	addWorkerFlags(workerCommand.Flags())
	for key, name := range map[string]string{
		"gateway.url": "gateway", "gateway.token": "token",
		"workspace.mirror_root": "mirror-root", "workspace.auto_send": "workspace-auto-send",
		"worker.outbox": "outbox", "worker.state_db": "state-db",
	} {
		mustBind(settings, key, workerCommand.Flags().Lookup(name))
	}
	command.AddCommand(workerCommand)
	command.AddCommand(gatewaycmd.NewGatewayCommand())
	command.AddCommand(newLocalCommand(settings))
	command.AddCommand(newShimCommand(settings))
	command.AddCommand(&cobra.Command{
		Use: "version", Short: "print version",
		Run: func(command *cobra.Command, _ []string) {
			fmt.Fprintln(command.OutOrStdout(), gatewaycmd.Version)
		},
	})
	return command
}

func addWorkerFlags(flags *pflag.FlagSet) {
	flags.String("gateway", "ws://127.0.0.1:8080/internal/v1/worker/ws", "gateway worker WebSocket URL")
	flags.String("token", "", "worker token")
	flags.String("mirror-root", "~/mirror", "worker-local workspace mirror root")
	flags.Bool("workspace-auto-send", false, "send clean mirror changes after live detection and fresh review")
	flags.String("outbox", "~/.human/worker-outbox.db", "private SQLite outbox for unacknowledged worker events")
	flags.String("state-db", "~/.human/worker-state.db", "private SQLite TUI recovery state; empty disables persistence")
}

func run(ctx context.Context, settings *viper.Viper) error {
	url := strings.TrimSpace(settings.GetString("gateway.url"))
	token := strings.TrimSpace(settings.GetString("gateway.token"))
	if url == "" {
		return errors.New("gateway URL is required")
	}
	if token == "" {
		return errors.New("worker token is required")
	}
	mirrorRoot, err := resolveMirrorRoot(settings.GetString("workspace.mirror_root"))
	if err != nil {
		return err
	}
	outboxPath, err := resolvePrivatePath(settings.GetString("worker.outbox"), "worker outbox")
	if err != nil {
		return err
	}
	statePath, err := resolveOptionalPrivatePath(settings.GetString("worker.state_db"), "worker state database")
	if err != nil {
		return err
	}
	worker, err := workerlib.Open(ctx, workerlib.Config{
		GatewayURL: url, Token: token, MirrorRoot: mirrorRoot,
		OutboxPath: outboxPath, StatePath: statePath, DisableState: statePath == "",
		WorkspaceAutoSend: settings.GetBool("workspace.auto_send"),
	})
	if err != nil {
		return err
	}
	defer worker.Close()
	_, err = worker.Run(ctx)
	return err
}

func resolveMirrorRoot(value string) (string, error) {
	return resolvePrivatePath(value, "workspace mirror root")
}

func resolveOptionalPrivatePath(value, description string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	return resolvePrivatePath(value, description)
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
	settings.SetConfigName("human")
	settings.AddConfigPath(".")
	if home, err := os.UserHomeDir(); err == nil {
		settings.AddConfigPath(filepath.Join(home, ".human"))
	}
	if err := settings.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return err
		}
	}
	return nil
}

func mustBind(settings *viper.Viper, key string, flag *pflag.Flag) {
	if err := settings.BindPFlag(key, flag); err != nil {
		panic(err)
	}
}
