// Package humandcmd assembles the humand daemon and administration CLI.
package humandcmd

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
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/anthropic"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/dialect/responses"
	"github.com/vibe-agi/human/internal/completion/gateway"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/ratelimit"
	"github.com/vibe-agi/human/internal/store/sqlite"
	completionworkerws "github.com/vibe-agi/human/internal/workerws"
)

const Version = "0.1.0-dev"

func New() *cobra.Command {
	settings := viper.New()
	settings.SetEnvPrefix("HUMAN")
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	settings.AutomaticEnv()

	root := &cobra.Command{
		Use:           "humand",
		Short:         "Human Agent gateway daemon",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(command *cobra.Command, _ []string) error {
			return loadConfig(settings)
		},
	}
	root.PersistentFlags().String("config", "", "configuration file (toml/yaml/json)")
	root.PersistentFlags().String("db", ".human/human.db", "SQLite database path")
	mustBind(settings, "config", root.PersistentFlags().Lookup("config"))
	mustBind(settings, "db", root.PersistentFlags().Lookup("db"))

	root.AddCommand(newServeCommand(settings))
	root.AddCommand(newTokenCommand(settings))
	root.AddCommand(newMigrateCommand(settings))
	root.AddCommand(&cobra.Command{
		Use: "version", Short: "print version",
		Run: func(command *cobra.Command, _ []string) {
			fmt.Fprintln(command.OutOrStdout(), Version)
		},
	})
	return root
}

func newServeCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{
		Use:   "serve",
		Short: "run model API and worker WebSocket endpoints",
		RunE: func(command *cobra.Command, _ []string) error {
			return serve(command.Context(), settings)
		},
	}
	flags := command.Flags()
	flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	flags.Int("queue-capacity", 32, "maximum admitted and active requests")
	flags.Duration("heartbeat", 15*time.Second, "SSE heartbeat interval")
	flags.Duration("max-pending", 10*time.Minute, "maximum time waiting for a human response")
	flags.Float64("rate-limit", ratelimit.DefaultRatePerSecond, "new requests admitted per second for each API key")
	flags.Int("rate-burst", ratelimit.DefaultBurst, "maximum per-key admission burst")
	flags.Bool("audit-payload", false, "retain request and response payloads in addition to metadata")
	flags.Duration("audit-payload-ttl", 7*24*time.Hour, "retention period for explicitly enabled audit payloads")
	flags.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
	mustBind(settings, "serve.listen", flags.Lookup("listen"))
	mustBind(settings, "serve.queue_capacity", flags.Lookup("queue-capacity"))
	mustBind(settings, "serve.heartbeat", flags.Lookup("heartbeat"))
	mustBind(settings, "serve.max_pending", flags.Lookup("max-pending"))
	mustBind(settings, "serve.rate_limit", flags.Lookup("rate-limit"))
	mustBind(settings, "serve.rate_burst", flags.Lookup("rate-burst"))
	mustBind(settings, "audit.payload", flags.Lookup("audit-payload"))
	mustBind(settings, "audit.payload_ttl", flags.Lookup("audit-payload-ttl"))
	mustBind(settings, "serve.shutdown_timeout", flags.Lookup("shutdown-timeout"))
	return command
}

func serve(ctx context.Context, settings *viper.Viper) error {
	databasePath, err := resolveDatabasePath(settings.GetString("db"))
	if err != nil {
		return err
	}
	database, err := sqlite.Open(ctx, databasePath)
	if err != nil {
		return err
	}
	defer database.Close()
	if _, err := database.PurgeExpiredAuditPayloads(ctx, time.Now()); err != nil {
		return fmt.Errorf("purge expired audit payloads: %w", err)
	}
	authenticator := auth.NewService(database)
	workerHub := hub.New(settings.GetInt("serve.queue_capacity"))
	completionWorkerServer, err := completionworkerws.New(
		completionworkerws.Config{}, authenticator, workerHub, database,
	)
	if err != nil {
		return err
	}
	gatewayServer, err := gateway.NewServer(gateway.Config{
		HeartbeatInterval: settings.GetDuration("serve.heartbeat"),
		MaxPending:        settings.GetDuration("serve.max_pending"),
		RateLimit: ratelimit.Config{
			RatePerSecond: settings.GetFloat64("serve.rate_limit"),
			Burst:         settings.GetInt("serve.rate_burst"),
		},
		AuditPayload:    settings.GetBool("audit.payload"),
		AuditPayloadTTL: settings.GetDuration("audit.payload_ttl"),
	}, database, authenticator, workerHub, adapter.NewDefaultRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
		"/v1/messages":         anthropic.New(),
		"/v1/responses":        responses.New(),
	})
	if err != nil {
		return err
	}
	runContext, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := gatewayServer.Recover(runContext); err != nil {
		return fmt.Errorf("recover incomplete completion requests: %w", err)
	}
	mux := http.NewServeMux()
	registerServeRoutes(mux, completionWorkerServer, gatewayServer)
	httpServer := &http.Server{
		Addr:              settings.GetString("serve.listen"),
		Handler:           requestLog(mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	go purgeAuditPayloads(runContext, database)
	errorsChannel := make(chan error, 1)
	go func() {
		slog.Info("humand listening", "address", httpServer.Addr)
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
			return fmt.Errorf("shutdown humand: %w", err)
		}
		return <-errorsChannel
	}
}

func registerServeRoutes(
	mux *http.ServeMux,
	completionWorker http.Handler,
	gateway http.Handler,
) {
	mux.Handle("/internal/v1/worker/ws", completionWorker)
	mux.Handle("/", gateway)
}

func purgeAuditPayloads(ctx context.Context, database *sqlite.Store) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := database.PurgeExpiredAuditPayloads(ctx, now); err != nil && ctx.Err() == nil {
				slog.Error("purge expired audit payloads", "error", err)
			}
		}
	}
}

func newTokenCommand(settings *viper.Viper) *cobra.Command {
	command := &cobra.Command{Use: "token", Short: "issue and revoke API tokens"}
	issue := &cobra.Command{
		Use: "issue", Short: "issue a caller or worker token",
		RunE: func(command *cobra.Command, _ []string) error {
			subject := strings.TrimSpace(settings.GetString("token.subject"))
			if subject == "" {
				return errors.New("token subject is required")
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
			if keyID == "" {
				return errors.New("token key id is required")
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

func newMigrateCommand(settings *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use: "migrate", Short: "create or upgrade the SQLite schema",
		RunE: func(command *cobra.Command, _ []string) error {
			databasePath, err := resolveDatabasePath(settings.GetString("db"))
			if err != nil {
				return err
			}
			database, err := sqlite.Open(command.Context(), databasePath)
			if err != nil {
				return err
			}
			return database.Close()
		},
	}
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
