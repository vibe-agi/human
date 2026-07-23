package humancmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
)

const defaultOpenCodeBaseURL = "http://127.0.0.1:19080/v1"

type openCodeConfig struct {
	Schema   string                      `json:"$schema"`
	Provider map[string]openCodeProvider `json:"provider"`
}

type openCodeProvider struct {
	NPM     string                   `json:"npm"`
	Name    string                   `json:"name"`
	Options openCodeProviderOptions  `json:"options"`
	Models  map[string]openCodeModel `json:"models"`
}

type openCodeProviderOptions struct {
	BaseURL string            `json:"baseURL"`
	APIKey  string            `json:"apiKey"`
	Timeout bool              `json:"timeout"`
	Headers map[string]string `json:"headers,omitempty"`
}

type openCodeModel struct {
	Name  string             `json:"name"`
	Limit openCodeModelLimit `json:"limit"`
}

type openCodeModelLimit struct {
	Context int `json:"context"`
	Output  int `json:"output"`
}

func newInitCommand() *cobra.Command {
	command := &cobra.Command{
		Use:               "init",
		Short:             "generate client Agent configuration",
		Args:              cobra.NoArgs,
		PersistentPreRunE: func(*cobra.Command, []string) error { return nil },
	}
	command.AddCommand(newInitOpenCodeCommand())
	return command
}

func newInitOpenCodeCommand() *cobra.Command {
	var baseURL string
	var outputPath string
	var force bool
	command := &cobra.Command{
		Use:   "opencode",
		Short: "generate an exact OpenCode 1.17.18 Workspace provider",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			payload, err := generateOpenCodeConfig(baseURL)
			if err != nil {
				return err
			}
			if strings.TrimSpace(outputPath) == "" {
				_, err = command.OutOrStdout().Write(payload)
				return err
			}
			path, err := writeGeneratedConfig(outputPath, payload, force)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "wrote OpenCode provider config: %s\n", path)
			return err
		},
	}
	command.Flags().StringVar(&baseURL, "base-url", defaultOpenCodeBaseURL, "Human OpenAI-compatible model base URL")
	command.Flags().StringVarP(&outputPath, "output", "o", "", "atomically write the generated config to this file instead of stdout")
	command.Flags().BoolVar(&force, "force", false, "atomically replace an existing output file")
	return command
}

func generateOpenCodeConfig(baseURLValue string) ([]byte, error) {
	baseURL, err := normalizeOpenCodeBaseURL(baseURLValue)
	if err != nil {
		return nil, err
	}
	config := openCodeConfig{
		Schema: "https://opencode.ai/config.json",
		Provider: map[string]openCodeProvider{
			"human": {
				NPM:  "@ai-sdk/openai-compatible",
				Name: "Human Agent (local)",
				Options: openCodeProviderOptions{
					BaseURL: baseURL,
					APIKey:  "{env:HUMAN_CALLER_TOKEN}",
					Timeout: false,
				},
				Models: map[string]openCodeModel{
					"human-expert": {
						Name:  "Human Expert",
						Limit: openCodeModelLimit{Context: 100000, Output: 4096},
					},
				},
			},
		},
	}
	payload, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode OpenCode provider config: %w", err)
	}
	return append(payload, '\n'), nil
}

func normalizeOpenCodeBaseURL(value string) (string, error) {
	value = strings.TrimRight(strings.TrimSpace(value), "/")
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("OpenCode base URL must be an absolute HTTP(S) URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("OpenCode base URL must use http or https")
	}
	if parsed.Scheme == "http" && !isLoopbackURLHost(parsed.Hostname()) {
		return "", errors.New("OpenCode base URL must use https for a non-loopback host")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("OpenCode base URL must not contain credentials, a query, or a fragment")
	}
	return value, nil
}

func isLoopbackURLHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "localhost" {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func writeGeneratedConfig(value string, payload []byte, force bool) (string, error) {
	path, err := filepath.Abs(strings.TrimSpace(value))
	if err != nil || strings.TrimSpace(value) == "" {
		return "", errors.New("OpenCode output path is required")
	}
	directory := filepath.Dir(path)
	info, err := os.Stat(directory)
	if err != nil {
		return "", fmt.Errorf("inspect OpenCode output directory: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("OpenCode output parent is not a directory")
	}
	if !force {
		if _, err := os.Lstat(path); err == nil {
			return "", fmt.Errorf("OpenCode output %s already exists; use --force to replace it", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect OpenCode output: %w", err)
		}
	}

	temporary, err := os.CreateTemp(directory, ".human-opencode-*")
	if err != nil {
		return "", fmt.Errorf("create temporary OpenCode config: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("restrict temporary OpenCode config: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("write temporary OpenCode config: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("sync temporary OpenCode config: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close temporary OpenCode config: %w", err)
	}

	if force {
		err = replaceFileAtomically(temporaryPath, path)
	} else {
		// A hard-link publishes the already-complete same-directory temporary
		// file without a check-then-rename overwrite race.
		err = os.Link(temporaryPath, path)
	}
	if err != nil {
		if !force && errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("OpenCode output %s already exists; use --force to replace it", path)
		}
		return "", fmt.Errorf("publish OpenCode config atomically: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("remove temporary OpenCode config link: %w", err)
	}
	if runtime.GOOS != "windows" {
		directoryHandle, err := os.Open(directory)
		if err != nil {
			return "", fmt.Errorf("open OpenCode output directory for sync: %w", err)
		}
		syncErr := directoryHandle.Sync()
		closeErr := directoryHandle.Close()
		if syncErr != nil {
			return "", fmt.Errorf("sync OpenCode output directory: %w", syncErr)
		}
		if closeErr != nil {
			return "", fmt.Errorf("close OpenCode output directory: %w", closeErr)
		}
	}
	return path, nil
}
