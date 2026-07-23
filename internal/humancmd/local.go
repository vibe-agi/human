package humancmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newLocalCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use: "local", Short: "run the model endpoint, SQLite, and browser human side in one process",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runLocal(command.Context(), command.OutOrStdout(), settings)
		},
	}
	flags := command.Flags()
	flags.String("listen", "127.0.0.1:19080", "loopback model API listen address")
	flags.Int("queue-capacity", 32, "in-memory assignment delivery buffer; durable requests remain in SQLite")
	flags.Duration("stream-write-timeout", 10*time.Second, "maximum time for one SSE write and flush")
	flags.Duration("max-pending", 10*time.Minute, "maximum time waiting for a Human response")
	flags.Duration("shutdown-timeout", 10*time.Second, "graceful local shutdown timeout")
	flags.Bool("reset-credentials", false, "rotate the persisted local caller credential")
	flags.String("web", "", "loopback address of the browser human side; the printed login URL carries a per-start session token, so keep it off shared terminals and proxy logs (default 127.0.0.1:19081)")
	flags.Bool("no-auto-title", false, "do not auto-answer tool-less chat requests with a derived title; route them to the human inbox instead")
	persistent := command.PersistentFlags()
	persistent.String("workspace", "~/human-workspace", "Human-side base directory; each new agent session gets a stable child working directory")
	persistent.String("db", automaticPrivatePath, "embedded SQLite database; auto uses OS user data isolated by workspace")
	persistent.String("credentials", automaticPrivatePath, "mode-0600 credential file; auto uses OS user data isolated by workspace")
	persistent.String("state-db", automaticPrivatePath, "private SQLite browser conversation state; auto uses OS user data, empty disables persistence")
	persistent.String("caller-subject", "local-caller", "stable local caller identity")
	persistent.String("worker-subject", "local-worker", "stable local worker identity")

	for key, name := range map[string]string{
		"local.listen":               "listen",
		"local.queue_capacity":       "queue-capacity",
		"local.stream_write_timeout": "stream-write-timeout",
		"local.max_pending":          "max-pending", "local.shutdown_timeout": "shutdown-timeout",
		"local.reset_credentials": "reset-credentials", "local.web": "web",
		"local.no_auto_title": "no-auto-title",
	} {
		mustBind(settings, key, flags.Lookup(name))
	}
	for key, name := range map[string]string{
		"local.workspace": "workspace", "local.db": "db", "local.credentials": "credentials",
		"local.state_db":       "state-db",
		"local.caller_subject": "caller-subject", "local.worker_subject": "worker-subject",
	} {
		mustBind(settings, key, persistent.Lookup(name))
	}
	credentialsCommand := &cobra.Command{
		Use: "credentials", Short: "print the local caller credential as JSON",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			workspaceRoot, err := resolveLocalWorkspace(settings.GetString("local.workspace"))
			if err != nil {
				return err
			}
			return printPublicCallerCredential(command, settings, workspaceRoot)
		},
	}
	credentialsCommand.Flags().Bool("token-only", false, "print only the caller token for shell command substitution")
	command.AddCommand(credentialsCommand)
	command.AddCommand(newLocalBackupCommand(settings))
	command.AddCommand(newLocalVerifyBackupCommand())
	command.AddCommand(newLocalRestoreCommand(settings))
	return command
}

func runLocal(ctx context.Context, output io.Writer, settings *viper.Viper) error {
	return runLocalPublicStack(ctx, output, settings)
}

func resolveLocalWorkspace(value string) (string, error) {
	absolute, err := resolvePrivatePath(value, "local workspace")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return "", fmt.Errorf("create local Human workspace: %w", err)
	}
	workspace, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve local Human workspace: %w", err)
	}
	return workspace, nil
}
