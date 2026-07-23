package humancmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	localruntime "github.com/vibe-agi/human/local"
)

// publicCallerTokenPath resolves the persisted caller-token file for the local
// public-stack composition.
func publicCallerTokenPath(settings *viper.Viper, workspaceRoot string) (string, error) {
	return resolveWorkspaceDataPath(
		settings.GetString("local.credentials"), "local credential file", "local", workspaceRoot, "credentials.json",
	)
}

// printPublicCallerCredential prints the persisted public-stack caller token in
// either shell-friendly or structured form.
func printPublicCallerCredential(command *cobra.Command, settings *viper.Viper, workspaceRoot string) error {
	path, err := publicCallerTokenPath(settings, workspaceRoot)
	if err != nil {
		return err
	}
	token, found, err := readPublicCredentials(path)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("local credentials do not exist; start `human local` first")
	}
	if tokenOnly, _ := command.Flags().GetBool("token-only"); tokenOnly {
		_, err := fmt.Fprintln(command.OutOrStdout(), token)
		return err
	}
	return json.NewEncoder(command.OutOrStdout()).Encode(map[string]string{"caller_token": token})
}

// runLocalPublicStack starts a local instance on the public stack with a single
// persisted loopback caller token — no gateway, no worker token, no two-phase
// rotation journal.
func runLocalPublicStack(ctx context.Context, output io.Writer, settings *viper.Viper) error {
	workspaceRoot, err := resolveLocalWorkspace(settings.GetString("local.workspace"))
	if err != nil {
		return err
	}
	databasePath, err := resolveWorkspaceDataPath(
		settings.GetString("local.db"), "local database", "local", workspaceRoot, "store.db",
	)
	if err != nil {
		return err
	}
	callerTokenPath, err := publicCallerTokenPath(settings, workspaceRoot)
	if err != nil {
		return err
	}
	statePath := ""
	stateDisabled := strings.TrimSpace(settings.GetString("local.state_db")) == ""
	if !stateDisabled {
		statePath, err = resolveWorkspaceDataPath(
			settings.GetString("local.state_db"), "local worker state database", "local", workspaceRoot, "workerkit-state.db",
		)
		if err != nil {
			return err
		}
	}
	callerToken, err := ensurePublicCallerToken(callerTokenPath, settings.GetBool("local.reset_credentials"))
	if err != nil {
		return err
	}

	config := localruntime.Config{
		HumanWorkspaceRoot:  workspaceRoot,
		ExistingCallerToken: callerToken,
		CallerSubject:       settings.GetString("local.caller_subject"),
		WorkerSubject:       settings.GetString("local.worker_subject"),
		Public: localruntime.PublicStackConfig{
			DatabasePath: databasePath, CallerWriteTimeout: settings.GetDuration("local.stream_write_timeout"),
			MaxPending: settings.GetDuration("local.max_pending"), AssignmentBuffer: settings.GetInt("local.queue_capacity"),
		},
		ListenAddress:       settings.GetString("local.listen"),
		ShutdownTimeout:     settings.GetDuration("local.shutdown_timeout"),
		WebListenAddress:    settings.GetString("local.web"),
		WebStatePath:        statePath,
		WebStateDisabled:    stateDisabled,
		WebDisableAutoTitle: settings.GetBool("local.no_auto_title"),
	}
	if strings.TrimSpace(config.WebListenAddress) == "" {
		config.WebListenAddress = "127.0.0.1:19081"
	}
	instance, err := localruntime.Open(ctx, config)
	if err != nil {
		return fmt.Errorf("open local public stack: %w", err)
	}
	defer instance.Close()

	fmt.Fprintf(output, "Human local is ready\nHuman workspace base: %s\nmodel base URL: %s/v1\ncaller credential: %s\n",
		workspaceRoot, instance.BaseURL(), callerTokenPath)
	fmt.Fprintln(output, "each workspace-capable agent session gets a stable child directory; select any existing repo for it in the Web UI")
	fmt.Fprintf(output, "human side (browser): %s\n", instance.WebURL())
	waitDone := make(chan error, 1)
	go func() { waitDone <- instance.Wait() }()
	select {
	case err := <-waitDone:
		return err
	case <-ctx.Done():
		return nil
	}
}
