// Package humanmcpcmd assembles the caller-local Human Agent MCP server.
package humanmcpcmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vibe-agi/human/internal/callerfs"
	"github.com/vibe-agi/human/internal/callershim"
	"github.com/vibe-agi/human/internal/humanmcp"
	"github.com/vibe-agi/human/internal/patchapply"
)

const Version = "0.1.0-dev"

func New() *cobra.Command {
	settings := viper.New()
	settings.SetEnvPrefix("HUMAN")
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	settings.AutomaticEnv()
	root := &cobra.Command{
		Use: "human-mcp", Short: "Caller-local MCP bridge for asynchronous human delegation",
		SilenceUsage: true, SilenceErrors: true,
		PersistentPreRunE: func(*cobra.Command, []string) error { return loadConfig(settings) },
		RunE: func(command *cobra.Command, _ []string) error {
			return run(command.Context(), settings)
		},
	}
	flags := root.Flags()
	root.PersistentFlags().String("config", "", "configuration file (toml/yaml/json)")
	flags.String("gateway", "http://127.0.0.1:8080", "humand A2A base URL")
	flags.String("token", "", "caller bearer token")
	flags.String("repo", ".", "caller git workspace used by human_result(apply=true)")
	flags.String("apply-state", "", "private apply ledger directory (default: <repo>/.git/human-agent/apply)")
	flags.Bool("dirty-commit", false, "commit only touched dirty paths before applying a delivery")
	flags.String("mergiraf", "", "mergiraf executable override; '-' disables structured merge")
	flags.Bool("allow-cross-origin-a2a", false, "allow Agent Card endpoints on another origin")
	flags.Bool("allow-insecure-a2a-http", false, "allow bearer-authenticated A2A over non-loopback plaintext HTTP")
	flags.Duration("http-timeout", 30*time.Second, "A2A HTTP request timeout")
	flags.Bool("allow-remote-exec", false, "register caller command tools and permit explicitly approved execution")
	flags.String("exec-ledger", "", "durable command execution ledger (default: shared git directory)")
	flags.Duration("exec-default-timeout", 30*time.Second, "default approved command timeout")
	flags.Duration("exec-max-timeout", 5*time.Minute, "maximum approved command timeout")
	flags.Int("exec-max-output", 1<<20, "maximum bytes retained from each command output stream")
	mustBind(settings, "config", root.PersistentFlags().Lookup("config"))
	mustBind(settings, "a2a.url", flags.Lookup("gateway"))
	mustBind(settings, "a2a.token", flags.Lookup("token"))
	mustBind(settings, "caller.repository", flags.Lookup("repo"))
	mustBind(settings, "caller.apply_state", flags.Lookup("apply-state"))
	mustBind(settings, "caller.dirty_commit", flags.Lookup("dirty-commit"))
	mustBind(settings, "caller.mergiraf", flags.Lookup("mergiraf"))
	mustBind(settings, "a2a.allow_cross_origin", flags.Lookup("allow-cross-origin-a2a"))
	mustBind(settings, "a2a.allow_insecure_http", flags.Lookup("allow-insecure-a2a-http"))
	mustBind(settings, "a2a.http_timeout", flags.Lookup("http-timeout"))
	mustBind(settings, "exec.enabled", flags.Lookup("allow-remote-exec"))
	mustBind(settings, "exec.ledger", flags.Lookup("exec-ledger"))
	mustBind(settings, "exec.default_timeout", flags.Lookup("exec-default-timeout"))
	mustBind(settings, "exec.max_timeout", flags.Lookup("exec-max-timeout"))
	mustBind(settings, "exec.max_output", flags.Lookup("exec-max-output"))
	root.AddCommand(&cobra.Command{
		Use: "version", Short: "print version",
		Run: func(command *cobra.Command, _ []string) {
			fmt.Fprintln(command.OutOrStdout(), Version)
		},
	})
	return root
}

func run(ctx context.Context, settings *viper.Viper) error {
	baseURL := strings.TrimSpace(settings.GetString("a2a.url"))
	token := strings.TrimSpace(settings.GetString("a2a.token"))
	if baseURL == "" || token == "" {
		return errors.New("A2A gateway URL and caller token are required")
	}
	repository, err := resolvePath(settings.GetString("caller.repository"), "caller repository", false)
	if err != nil {
		return err
	}
	stateRoot, err := resolvePath(settings.GetString("caller.apply_state"), "apply state", true)
	if err != nil {
		return err
	}
	applyEngine, err := patchapply.Open(patchapply.Config{
		Repository: repository, StateRoot: stateRoot,
		DirtyCommit:  settings.GetBool("caller.dirty_commit"),
		MergirafPath: settings.GetString("caller.mergiraf"),
	})
	if err != nil {
		return fmt.Errorf("open caller patch engine: %w", err)
	}
	var execRunner humanmcp.ExecRunner
	var execLedger *callershim.SQLiteLedger
	if settings.GetBool("exec.enabled") {
		root, err := callerfs.OpenRoot(repository)
		if err != nil {
			return fmt.Errorf("open caller execution root: %w", err)
		}
		ledgerPath, err := commandLedgerPath(ctx, repository, settings.GetString("exec.ledger"))
		if err != nil {
			return err
		}
		execLedger, err = callershim.OpenSQLiteLedger(ctx, ledgerPath)
		if err != nil {
			return fmt.Errorf("open caller command ledger: %w", err)
		}
		defer execLedger.Close()
		executor, err := callershim.NewExecutor(callershim.ExecutorConfig{
			Root: root, Ledger: execLedger, ExecEnabled: true,
			DefaultTimeout: settings.GetDuration("exec.default_timeout"),
			MaxTimeout:     settings.GetDuration("exec.max_timeout"),
			MaxOutputBytes: settings.GetInt("exec.max_output"),
		})
		if err != nil {
			return fmt.Errorf("create caller command executor: %w", err)
		}
		identity := sha256.Sum256([]byte(baseURL + "\x00" + root.Path()))
		execRunner, err = humanmcp.NewCallerExecRunner(executor, "human-mcp:"+hex.EncodeToString(identity[:]))
		if err != nil {
			return err
		}
	}
	authority, err := humanmcp.NewA2AAuthority(ctx, humanmcp.A2AConfig{
		BaseURL: baseURL, BearerToken: token,
		HTTPClient:        &http.Client{Timeout: settings.GetDuration("a2a.http_timeout")},
		AllowCrossOrigin:  settings.GetBool("a2a.allow_cross_origin"),
		AllowInsecureHTTP: settings.GetBool("a2a.allow_insecure_http"),
	})
	if err != nil {
		return err
	}
	server, err := humanmcp.NewServer(humanmcp.Config{
		Authority: authority, PatchApply: applyEngine, ExecRunner: execRunner, Version: Version,
	})
	if err != nil {
		return err
	}
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		return fmt.Errorf("serve Human Agent MCP over stdio: %w", err)
	}
	return nil
}

func commandLedgerPath(ctx context.Context, repository, configured string) (string, error) {
	if strings.TrimSpace(configured) != "" {
		path, err := resolvePath(configured, "command execution ledger", false)
		if err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return "", fmt.Errorf("create command ledger directory: %w", err)
		}
		return path, nil
	}
	command := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "--git-common-dir")
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("resolve shared git directory for command ledger: %w", err)
	}
	common := strings.TrimSpace(string(output))
	if !filepath.IsAbs(common) {
		common = filepath.Join(repository, common)
	}
	common, err = filepath.Abs(common)
	if err != nil {
		return "", err
	}
	directory := filepath.Join(common, "human-agent")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create command ledger directory: %w", err)
	}
	return filepath.Join(directory, "exec.db"), nil
}

func resolvePath(value, description string, optional bool) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" && optional {
		return "", nil
	}
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
