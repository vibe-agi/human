// Package humancmd assembles the human worker TUI command.
package humancmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/tui"
	"github.com/vibe-agi/human/internal/workerclient"
)

func New() *cobra.Command {
	settings := viper.New()
	settings.SetEnvPrefix("HUMAN")
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	settings.AutomaticEnv()
	command := &cobra.Command{
		Use: "human", Short: "Human Agent worker TUI",
		SilenceUsage: true, SilenceErrors: true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error { return loadConfig(settings) },
		RunE: func(command *cobra.Command, _ []string) error {
			return run(command.Context(), settings)
		},
	}
	command.PersistentFlags().String("config", "", "configuration file (toml/yaml/json)")
	command.Flags().String("gateway", "ws://127.0.0.1:8080/internal/v1/worker/ws", "humand worker WebSocket URL")
	command.Flags().String("token", "", "worker token")
	command.Flags().String("mirror-root", "~/mirror", "worker-local workspace mirror root")
	command.Flags().String("outbox", "~/.human/worker-outbox.db", "private SQLite outbox for unacknowledged worker events")
	mustBind(settings, "config", command.PersistentFlags().Lookup("config"))
	mustBind(settings, "gateway.url", command.Flags().Lookup("gateway"))
	mustBind(settings, "gateway.token", command.Flags().Lookup("token"))
	mustBind(settings, "workspace.mirror_root", command.Flags().Lookup("mirror-root"))
	mustBind(settings, "worker.outbox", command.Flags().Lookup("outbox"))
	command.AddCommand(newShimCommand(settings))
	command.AddCommand(newDelegationCommand(settings))
	return command
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
	client, err := workerclient.DialWithOutbox(ctx, url, token, outboxPath)
	if err != nil {
		return fmt.Errorf("connect worker WebSocket: %w", err)
	}
	defer client.Close()
	_, err = tea.NewProgram(tui.New(client, tui.WithMirrorRoot(mirrorRoot))).Run()
	return err
}

func resolveMirrorRoot(value string) (string, error) {
	return resolvePrivatePath(value, "workspace mirror root")
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
