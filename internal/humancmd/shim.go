package humancmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/callershim"
)

func newShimCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use:   "shim",
		Short: "run the demand-side model proxy and tool execution boundary",
		RunE: func(command *cobra.Command, _ []string) error {
			return runShim(command.Context(), settings)
		},
	}
	flags := command.Flags()
	flags.String("listen", "127.0.0.1:8181", "local shim listen address")
	flags.String("upstream", "http://127.0.0.1:8080", "humand model API base URL")
	flags.String("caller-token", "", "humand caller token")
	flags.String("tool-token", "", "local tool endpoint bearer token")
	flags.String("caller-id", "", "stable caller id matching the humand caller principal; retained across token rotation")
	flags.String("workspace", ".", "caller workspace root")
	flags.String("workspace-key", "", "stable caller workspace key")
	flags.String("task-id", "", "stable task id; reuse it with the same ledger across shim restarts")
	flags.String("ledger", ".human/caller-ledger.db", "durable caller tool ledger path")
	flags.Bool("allow-exec", false, "enable bounded remote command tool calls for this task")
	flags.Duration("exec-timeout", 30*time.Second, "default remote command timeout")
	flags.Duration("exec-max-timeout", 5*time.Minute, "maximum remote command timeout")
	flags.Int("exec-max-output", 1<<20, "maximum bytes retained for each stdout/stderr stream")
	flags.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
	mustBind(settings, "shim.listen", flags.Lookup("listen"))
	mustBind(settings, "shim.upstream", flags.Lookup("upstream"))
	mustBind(settings, "shim.caller_token", flags.Lookup("caller-token"))
	mustBind(settings, "shim.tool_token", flags.Lookup("tool-token"))
	mustBind(settings, "shim.caller_id", flags.Lookup("caller-id"))
	mustBind(settings, "shim.workspace", flags.Lookup("workspace"))
	mustBind(settings, "shim.workspace_key", flags.Lookup("workspace-key"))
	mustBind(settings, "shim.task_id", flags.Lookup("task-id"))
	mustBind(settings, "shim.ledger", flags.Lookup("ledger"))
	mustBind(settings, "shim.allow_exec", flags.Lookup("allow-exec"))
	mustBind(settings, "shim.exec_timeout", flags.Lookup("exec-timeout"))
	mustBind(settings, "shim.exec_max_timeout", flags.Lookup("exec-max-timeout"))
	mustBind(settings, "shim.exec_max_output", flags.Lookup("exec-max-output"))
	mustBind(settings, "shim.shutdown_timeout", flags.Lookup("shutdown-timeout"))
	return command
}

func runShim(ctx context.Context, settings *viper.Viper) error {
	callerToken := strings.TrimSpace(settings.GetString("shim.caller_token"))
	toolToken := strings.TrimSpace(settings.GetString("shim.tool_token"))
	callerID := strings.TrimSpace(settings.GetString("shim.caller_id"))
	workspaceKey := strings.TrimSpace(settings.GetString("shim.workspace_key"))
	taskID := strings.TrimSpace(settings.GetString("shim.task_id"))
	if callerToken == "" || toolToken == "" || callerID == "" || workspaceKey == "" || taskID == "" {
		return errors.New("shim caller-token, tool-token, caller-id, workspace-key, and task-id are required")
	}
	root, err := callerfs.OpenRoot(settings.GetString("shim.workspace"))
	if err != nil {
		return err
	}
	ledgerPath, err := filepath.Abs(settings.GetString("shim.ledger"))
	if err != nil {
		return fmt.Errorf("resolve caller ledger path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(ledgerPath), 0o700); err != nil {
		return fmt.Errorf("create caller ledger directory: %w", err)
	}
	ledger, err := callershim.OpenSQLiteLedger(ctx, ledgerPath)
	if err != nil {
		return err
	}
	defer ledger.Close()
	executor, err := callershim.NewExecutor(callershim.ExecutorConfig{
		Root: root, Ledger: ledger, ExecEnabled: settings.GetBool("shim.allow_exec"),
		DefaultTimeout: settings.GetDuration("shim.exec_timeout"),
		MaxTimeout:     settings.GetDuration("shim.exec_max_timeout"),
		MaxOutputBytes: settings.GetInt("shim.exec_max_output"),
	})
	if err != nil {
		return err
	}
	shim, err := callershim.NewServer(callershim.ServerConfig{
		GatewayURL: settings.GetString("shim.upstream"), CallerToken: callerToken, ToolToken: toolToken,
		CallerID: callerID, WorkspaceKey: workspaceKey, WorkspaceRoot: root.Path(), TaskID: taskID,
		AllowExec: settings.GetBool("shim.allow_exec"), Executor: executor,
	})
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr: settings.GetString("shim.listen"), Handler: shim,
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
	}
	runContext, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsChannel := make(chan error, 1)
	go func() {
		slog.Info("caller shim listening", "address", httpServer.Addr, "default_task_id", taskID, "workspace_key", workspaceKey)
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errorsChannel <- err
	}()
	select {
	case err := <-errorsChannel:
		return err
	case <-runContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), settings.GetDuration("shim.shutdown_timeout"))
		defer cancel()
		if err := httpServer.Shutdown(shutdownContext); err != nil {
			return err
		}
		return <-errorsChannel
	}
}
