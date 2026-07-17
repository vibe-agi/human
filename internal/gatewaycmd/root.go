// Package gatewaycmd assembles the standalone gateway and administration CLI.
package gatewaycmd

import (
	"context"
	"encoding/json"
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
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	publicgateway "github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/ratelimit"
	"github.com/vibe-agi/human/internal/store/sqlite"
)

const Version = "0.1.0-dev"

// NewGatewayCommand returns the `human gateway` standalone server and its
// nested token administration commands.
func NewGatewayCommand() *cobra.Command {
	settings := newSettings()
	command := &cobra.Command{
		Use: "gateway", Short: "run the standalone Human Agent gateway",
		SilenceUsage: true, SilenceErrors: true,
		PersistentPreRunE: func(command *cobra.Command, _ []string) error {
			if configFlag := command.Flag("config"); configFlag != nil && configFlag.Value.String() != "" {
				settings.Set("config", configFlag.Value.String())
			}
			return loadConfig(settings)
		},
		RunE: func(command *cobra.Command, _ []string) error {
			return serve(command.Context(), settings)
		},
	}
	command.PersistentFlags().String("db", ".human/human.db", "SQLite database path")
	mustBind(settings, "db", command.PersistentFlags().Lookup("db"))
	addServeFlags(command, settings)
	command.AddCommand(newTokenCommand(settings))
	return command
}

func newSettings() *viper.Viper {
	settings := viper.New()
	settings.SetEnvPrefix("HUMAN")
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	settings.AutomaticEnv()
	return settings
}

func addServeFlags(command *cobra.Command, settings *viper.Viper) {
	flags := command.Flags()
	flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	flags.Int("queue-capacity", 32, "maximum admitted and active requests")
	flags.Duration("heartbeat", 15*time.Second, "SSE heartbeat interval")
	flags.Duration("stream-write-timeout", 10*time.Second, "maximum time for one SSE write and flush")
	flags.Duration("max-pending", 10*time.Minute, "maximum time waiting for a human response")
	flags.Bool("disable-codex-auto-idempotency", false, "disable automatic retry identity for recognized Codex turns")
	flags.Float64("rate-limit", ratelimit.DefaultRatePerSecond, "new requests admitted per second for each API key")
	flags.Int("rate-burst", ratelimit.DefaultBurst, "maximum per-key admission burst")
	flags.Bool("audit-payload", false, "retain request and response payloads in addition to metadata")
	flags.Duration("audit-payload-ttl", 7*24*time.Hour, "retention period for explicitly enabled audit payloads")
	flags.Duration("replay-payload-grace", 24*time.Hour, "exact idempotent replay retention after request completion")
	flags.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
	mustBind(settings, "serve.listen", flags.Lookup("listen"))
	mustBind(settings, "serve.queue_capacity", flags.Lookup("queue-capacity"))
	mustBind(settings, "serve.heartbeat", flags.Lookup("heartbeat"))
	mustBind(settings, "serve.stream_write_timeout", flags.Lookup("stream-write-timeout"))
	mustBind(settings, "serve.max_pending", flags.Lookup("max-pending"))
	mustBind(settings, "serve.disable_codex_auto_idempotency", flags.Lookup("disable-codex-auto-idempotency"))
	mustBind(settings, "serve.rate_limit", flags.Lookup("rate-limit"))
	mustBind(settings, "serve.rate_burst", flags.Lookup("rate-burst"))
	mustBind(settings, "audit.payload", flags.Lookup("audit-payload"))
	mustBind(settings, "audit.payload_ttl", flags.Lookup("audit-payload-ttl"))
	mustBind(settings, "serve.replay_payload_grace", flags.Lookup("replay-payload-grace"))
	mustBind(settings, "serve.shutdown_timeout", flags.Lookup("shutdown-timeout"))
}

func serve(ctx context.Context, settings *viper.Viper) error {
	replayPayloadGrace := settings.GetDuration("serve.replay_payload_grace")
	if replayPayloadGrace <= 0 {
		return errors.New("replay payload grace must be positive")
	}
	databasePath, err := resolveDatabasePath(settings.GetString("db"))
	if err != nil {
		return err
	}
	runContext, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	config := publicgateway.DefaultConfig()
	config.DatabasePath = databasePath
	config.QueueCapacity = settings.GetInt("serve.queue_capacity")
	config.HeartbeatInterval = settings.GetDuration("serve.heartbeat")
	config.StreamWriteTimeout = settings.GetDuration("serve.stream_write_timeout")
	config.MaxPending = settings.GetDuration("serve.max_pending")
	config.DisableCodexAutoIdempotency = settings.GetBool("serve.disable_codex_auto_idempotency")
	config.RateLimit.RatePerSecond = settings.GetFloat64("serve.rate_limit")
	config.RateLimit.Burst = settings.GetInt("serve.rate_burst")
	config.AuditPayload = settings.GetBool("audit.payload")
	config.AuditPayloadTTL = settings.GetDuration("audit.payload_ttl")
	config.ReplayPayloadGrace = replayPayloadGrace
	gatewayServer, err := publicgateway.Open(runContext, config)
	if err != nil {
		return err
	}
	defer gatewayServer.Close()
	httpServer := &http.Server{
		Addr:              settings.GetString("serve.listen"),
		Handler:           requestLog(gatewayServer),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	errorsChannel := make(chan error, 1)
	go func() {
		slog.Info("human gateway listening", "address", httpServer.Addr)
		err := httpServer.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			errorsChannel <- err
			return
		}
		errorsChannel <- nil
	}()
	select {
	case err := <-errorsChannel:
		return err
	case <-runContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), settings.GetDuration("serve.shutdown_timeout"))
		defer cancel()
		if err := httpServer.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("shutdown gateway: %w", err)
		}
		return <-errorsChannel
	}
}

func newTokenCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{Use: "token", Short: "issue and revoke API tokens"}
	issue := &cobra.Command{
		Use: "issue", Short: "issue a caller or worker token",
		RunE: func(command *cobra.Command, _ []string) error {
			subject := strings.TrimSpace(settings.GetString("token.subject"))
			if !completion.IsStableKey(subject) {
				return errors.New("token subject must be a stable key")
			}
			database, err := openDatabase(command.Context(), settings.GetString("db"))
			if err != nil {
				return err
			}
			defer database.Close()
			principalType := auth.PrincipalType(settings.GetString("token.type"))
			issued, err := auth.NewService(database).Issue(command.Context(), principalType, subject)
			if err != nil {
				return err
			}
			return json.NewEncoder(command.OutOrStdout()).Encode(map[string]string{
				"key_id": issued.KeyID, "token": issued.Secret,
			})
		},
	}
	issue.Flags().String("type", "caller", "principal type: caller or worker")
	issue.Flags().String("subject", "", "caller_id or worker_id")
	mustBind(settings, "token.type", issue.Flags().Lookup("type"))
	mustBind(settings, "token.subject", issue.Flags().Lookup("subject"))

	revoke := &cobra.Command{
		Use: "revoke", Short: "revoke an API token by key id",
		RunE: func(command *cobra.Command, _ []string) error {
			keyID := strings.TrimSpace(settings.GetString("token.key_id"))
			if !completion.IsStableKey(keyID) {
				return errors.New("token key id must be a stable key")
			}
			database, err := openDatabase(command.Context(), settings.GetString("db"))
			if err != nil {
				return err
			}
			defer database.Close()
			return auth.NewService(database).Revoke(command.Context(), keyID)
		},
	}
	revoke.Flags().String("key-id", "", "token key id")
	mustBind(settings, "token.key_id", revoke.Flags().Lookup("key-id"))
	command.AddCommand(issue, revoke)
	return command
}

func openDatabase(ctx context.Context, path string) (*sqlite.Store, error) {
	path, err := resolveDatabasePath(path)
	if err != nil {
		return nil, err
	}
	return sqlite.Open(ctx, path)
}

func resolveDatabasePath(path string) (string, error) {
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("resolve database path: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
			return "", fmt.Errorf("create database directory: %w", err)
		}
		path = absolute
	}
	return path, nil
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

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		started := time.Now()
		next.ServeHTTP(response, request)
		slog.Info("http request", "method", request.Method, "path", request.URL.Path, "duration", time.Since(started))
	})
}
