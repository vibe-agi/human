// Package local runs the batteries-included HumanLLM desktop composition in
// one process: a loopback caller endpoint, an SQLite-backed llm.Service, and a
// browser human side connected in-process through workerkit.
package local

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
	workerkitsqlite "github.com/vibe-agi/human/workerkit/sqlite"
)

const (
	// DefaultListenAddress asks the kernel for a free IPv4 loopback port.
	DefaultListenAddress = "127.0.0.1:0"
	defaultCallerSubject = "local-caller"
	defaultWorkerSubject = "local-worker"
	defaultShutdownWait  = 5 * time.Second
)

// Config controls the reference one-process deployment. It intentionally
// exposes only policy and durable locations owned by this composition. Hosts
// needing different authentication, stores, routing, transports, or UIs
// compose the public llm, callerhttp, and workerkit ports directly.
type Config struct {
	Public PublicStackConfig

	ListenAddress   string
	CallerSubject   string
	WorkerSubject   string
	ShutdownTimeout time.Duration

	// WebListenAddress is the loopback address of the browser human side.
	// Empty asks the kernel for a free loopback port.
	WebListenAddress string
	// WebStatePath is the durable workerkit conversation state used by the web
	// human side. Empty derives workerkit-state.db in workspace-scoped user data.
	WebStatePath string
	// WebStateStore optionally supplies an application-owned implementation.
	// Local borrows it and ignores WebStatePath. The built-in implementation is
	// SQLite; this is the replacement seam for embedders.
	WebStateStore workerkit.StateStore
	// WebStateDisabled selects the reference in-memory implementation and turns
	// off the filesystem mirror baseline.
	WebStateDisabled bool
	// WebDisableAutoTitle routes tool-less chat requests to the inbox instead of
	// using the built-in title responder.
	WebDisableAutoTitle bool

	// HumanWorkspaceRoot is the Human-side base directory. New workspace-tier
	// conversations receive stable child directories here; the Human may later
	// select a different existing repo per conversation in the Web UI.
	// It never describes the Agent user's cwd.
	HumanWorkspaceRoot string

	// ExistingCallerToken reuses a host-owned bearer/API key. Empty creates an
	// ephemeral token returned by CallerToken/Credentials; Local never persists
	// or revokes host credentials.
	ExistingCallerToken string
}

// PublicStackConfig selects operational policy for the reference composition.
// Scheduling remains a host concern: Local drives the public RunExpiry and
// RunRetention operations using these intervals.
type PublicStackConfig struct {
	DatabasePath           string
	CallerWriteTimeout     time.Duration
	CallerHeartbeat        time.Duration
	MaxPending             time.Duration
	ExpirySweepInterval    time.Duration
	ReplayPayloadGrace     time.Duration
	RetentionSweepInterval time.Duration
	AssignmentBuffer       int
}

// Credentials are the caller-side secret for the local model endpoint.
// NewlyIssued is true only for an ephemeral token allocated by Open. The
// in-process worker has no credential surface.
type Credentials struct {
	CallerToken string
	NewlyIssued bool
}

// DefaultConfig returns a complete workspace-scoped public-stack
// configuration. Private databases live in OS user data; the Human workspace
// base is the only intentionally user-visible tree.
func DefaultConfig() (Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, fmt.Errorf("resolve default Human workspace: %w", err)
	}
	workspaceRoot, err := resolveHumanWorkspaceRoot(filepath.Join(home, "human-workspace"))
	if err != nil {
		return Config{}, err
	}
	return defaultsForHumanWorkspace(workspaceRoot)
}

func defaultsForHumanWorkspace(workspaceRoot string) (Config, error) {
	storePath, err := userdata.WorkspacePath("local", workspaceRoot, "store.db")
	if err != nil {
		return Config{}, fmt.Errorf("resolve default local service path: %w", err)
	}
	statePath, err := userdata.WorkspacePath("local", workspaceRoot, "workerkit-state.db")
	if err != nil {
		return Config{}, fmt.Errorf("resolve default local web state path: %w", err)
	}
	return Config{
		Public: PublicStackConfig{
			DatabasePath: storePath, CallerWriteTimeout: 10 * time.Second,
			CallerHeartbeat: 15 * time.Second, MaxPending: 10 * time.Minute,
			ExpirySweepInterval: 30 * time.Second, ReplayPayloadGrace: 24 * time.Hour,
			RetentionSweepInterval: time.Hour, AssignmentBuffer: 32,
		},
		ListenAddress:      DefaultListenAddress,
		CallerSubject:      defaultCallerSubject,
		WorkerSubject:      defaultWorkerSubject,
		ShutdownTimeout:    defaultShutdownWait,
		WebListenAddress:   "127.0.0.1:0",
		WebStatePath:       statePath,
		HumanWorkspaceRoot: workspaceRoot,
	}, nil
}

func (config Config) withDefaults() (Config, error) {
	if strings.TrimSpace(config.HumanWorkspaceRoot) == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Config{}, fmt.Errorf("resolve default Human workspace: %w", err)
		}
		config.HumanWorkspaceRoot = filepath.Join(home, "human-workspace")
	}
	var err error
	config.HumanWorkspaceRoot, err = resolveHumanWorkspaceRoot(config.HumanWorkspaceRoot)
	if err != nil {
		return Config{}, err
	}
	defaults, err := defaultsForHumanWorkspace(config.HumanWorkspaceRoot)
	if err != nil {
		return Config{}, err
	}
	if strings.TrimSpace(config.Public.DatabasePath) == "" {
		config.Public.DatabasePath, err = userdata.WorkspacePath("local", config.HumanWorkspaceRoot, "store.db")
		if err != nil {
			return Config{}, fmt.Errorf("resolve local service path: %w", err)
		}
	}
	if strings.TrimSpace(config.ListenAddress) == "" {
		config.ListenAddress = defaults.ListenAddress
	}
	if strings.TrimSpace(config.CallerSubject) == "" {
		config.CallerSubject = defaults.CallerSubject
	}
	if strings.TrimSpace(config.WorkerSubject) == "" {
		config.WorkerSubject = defaults.WorkerSubject
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaults.ShutdownTimeout
	}
	if config.ShutdownTimeout < 0 {
		return Config{}, errors.New("open local: shutdown timeout must be positive")
	}
	if strings.TrimSpace(config.WebListenAddress) == "" {
		config.WebListenAddress = defaults.WebListenAddress
	}
	if config.WebStateDisabled && config.WebStateStore != nil {
		return Config{}, errors.New("open local: WebStateDisabled and WebStateStore are mutually exclusive")
	}
	if !config.WebStateDisabled && config.WebStateStore == nil && strings.TrimSpace(config.WebStatePath) == "" {
		config.WebStatePath, err = userdata.WorkspacePath("local", config.HumanWorkspaceRoot, "workerkit-state.db")
		if err != nil {
			return Config{}, fmt.Errorf("resolve local web state path: %w", err)
		}
	}
	if config.Public.CallerWriteTimeout == 0 {
		config.Public.CallerWriteTimeout = defaults.Public.CallerWriteTimeout
	}
	if config.Public.CallerHeartbeat == 0 {
		config.Public.CallerHeartbeat = defaults.Public.CallerHeartbeat
	}
	if config.Public.MaxPending == 0 {
		config.Public.MaxPending = defaults.Public.MaxPending
	}
	if config.Public.ExpirySweepInterval == 0 {
		config.Public.ExpirySweepInterval = defaults.Public.ExpirySweepInterval
	}
	if config.Public.ReplayPayloadGrace == 0 {
		config.Public.ReplayPayloadGrace = defaults.Public.ReplayPayloadGrace
	}
	if config.Public.RetentionSweepInterval == 0 {
		config.Public.RetentionSweepInterval = defaults.Public.RetentionSweepInterval
	}
	if config.Public.CallerWriteTimeout < 0 || config.Public.CallerHeartbeat < 0 ||
		config.Public.MaxPending < 0 || config.Public.ExpirySweepInterval < 0 ||
		config.Public.ReplayPayloadGrace < 0 || config.Public.RetentionSweepInterval < 0 {
		return Config{}, errors.New("open local: public stack durations must be positive")
	}
	if config.Public.AssignmentBuffer < 0 || config.Public.AssignmentBuffer > 4096 {
		return Config{}, errors.New("open local: assignment buffer must be 1..4096 (or zero for the default)")
	}
	config.ListenAddress = strings.TrimSpace(config.ListenAddress)
	config.WebListenAddress = strings.TrimSpace(config.WebListenAddress)
	config.CallerSubject = strings.TrimSpace(config.CallerSubject)
	config.WorkerSubject = strings.TrimSpace(config.WorkerSubject)
	config.ExistingCallerToken = strings.TrimSpace(config.ExistingCallerToken)
	return config, nil
}

func resolveHumanWorkspaceRoot(value string) (string, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(value))
	if err != nil || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("resolve Human workspace: path is required")
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return "", fmt.Errorf("create Human workspace: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve Human workspace: %w", err)
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("resolve Human workspace: path is not a directory")
	}
	return canonical, nil
}

// Local owns the reference service, caller transport, loopback HTTP servers,
// workerkit worker, mirror, and built-in StateStore resource. Close is
// idempotent.
type Local struct {
	service       *llm.Service
	callerRuntime llm.CallerTransportRuntime

	webMirror       *fsmirror.Mirror
	webWorker       *workerkit.Worker
	webServer       *web.Server
	webHTTP         *http.Server
	webListener     net.Listener
	webStateRelease framework.ReleaseFunc
	webURL          string

	httpServer  *http.Server
	listener    net.Listener
	baseURL     string
	credentials Credentials

	runContext      context.Context
	cancel          context.CancelFunc
	shutdownTimeout time.Duration

	serveDone chan struct{}
	serveMu   sync.Mutex
	serveErr  error

	closeOnce sync.Once
	closeErr  error
}

// Open starts the public reference composition. Any partial failure closes all
// components already opened. The local package is intentionally in-process;
// distributed gateway/worker transports are composed by their own products.
func Open(ctx context.Context, config Config) (*Local, error) {
	if ctx == nil {
		return nil, errors.New("open local: context is required")
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := rejectPendingOfflineRestore(config.Public.DatabasePath); err != nil {
		return nil, err
	}
	return openPublicStackInstance(ctx, config)
}

func (local *Local) openWebState(ctx context.Context, config Config) (workerkit.StateStore, string, error) {
	if config.WebStateStore != nil {
		return config.WebStateStore, "", nil
	}
	if config.WebStateDisabled {
		state, release := workerkit.NewMemoryStateStore()
		local.webStateRelease = release
		return state, "", nil
	}
	stateResource, err := workerkitsqlite.Open(ctx, workerkitsqlite.Config{Path: config.WebStatePath})
	if err != nil {
		return nil, "", fmt.Errorf("open local web worker state: %w", err)
	}
	local.webStateRelease = stateResource.Release
	stateStore, err := stateResource.Value()
	if err != nil {
		_ = stateResource.Release(context.Background())
		local.webStateRelease = nil
		return nil, "", fmt.Errorf("acquire local web worker state: %w", err)
	}
	return stateStore, filepath.Join(filepath.Dir(config.WebStatePath), "workerkit-mirror-baseline.json"), nil
}

// autoTitleResponder answers tool-less chat assignments (OpenCode's hidden
// title/summary generation) so only real turns reach the inbox.
func autoTitleResponder(delivery llm.WorkerAssignmentDelivery) (string, bool) {
	if len(delivery.Assignment.Request.Tools) != 0 ||
		delivery.Assignment.Task.CapabilityTier != llm.TierChat {
		return "", false
	}
	var latest string
	for _, message := range delivery.Assignment.Request.Messages {
		if message.Role != llm.RoleUser {
			continue
		}
		for _, block := range message.Blocks {
			if block.Type == llm.BlockText && strings.TrimSpace(block.Text) != "" {
				latest = block.Text
			}
		}
	}
	title := strings.TrimSpace(strings.SplitN(latest, "\n", 2)[0])
	if runes := []rune(title); len(runes) > 60 {
		title = string(runes[:60])
	}
	if title == "" {
		title = "Conversation"
	}
	return title, true
}

// WebURL returns the browser login URL including its per-start session token.
func (local *Local) WebURL() string {
	if local == nil {
		return ""
	}
	return local.webURL
}

func rejectPendingOfflineRestore(databasePath string) error {
	trimmed := strings.TrimSpace(databasePath)
	if trimmed != ":memory:" && !strings.HasPrefix(strings.ToLower(trimmed), "file:") {
		absolute, err := filepath.Abs(trimmed)
		if err != nil {
			return fmt.Errorf("inspect pending local restore: %w", err)
		}
		if _, err := os.Stat(filepath.Dir(absolute)); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return fmt.Errorf("inspect pending local restore directory: %w", err)
		}
	}
	location, err := sqlitefile.Resolve(databasePath)
	if err != nil {
		return fmt.Errorf("inspect pending local restore: %w", err)
	}
	if !location.FileBacked {
		return nil
	}
	journalPath := location.Path + ".restore-journal.json"
	if _, err := os.Lstat(journalPath); err == nil {
		return fmt.Errorf("open local: an interrupted offline restore is pending at %s; resume it before starting Human local", journalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect pending local restore journal: %w", err)
	}
	return nil
}

// BaseURL returns the loopback model API base URL.
func (local *Local) BaseURL() string {
	if local == nil {
		return ""
	}
	return local.baseURL
}

// CallerToken returns the caller bearer/API key held in memory.
func (local *Local) CallerToken() string {
	if local == nil {
		return ""
	}
	return local.credentials.CallerToken
}

// Credentials returns a copy of the local caller credential metadata.
func (local *Local) Credentials() Credentials {
	if local == nil {
		return Credentials{}
	}
	return local.credentials
}

// Service returns the public correctness core owned by Local. Callers may use
// its read-only/status and explicit maintenance APIs while Local is alive; they
// must not shut it down independently.
func (local *Local) Service() *llm.Service {
	if local == nil {
		return nil
	}
	return local.service
}

// WebWorker returns the headless human-side domain object behind the stock UI.
func (local *Local) WebWorker() *workerkit.Worker {
	if local == nil {
		return nil
	}
	return local.webWorker
}

// Wait waits for the model HTTP server to stop.
func (local *Local) Wait() error {
	if local == nil || local.serveDone == nil {
		return nil
	}
	<-local.serveDone
	return local.loadServeError()
}

// Close cancels active handlers and maintenance loops, drains both loopback
// servers, stops the human side, and finally shuts down Service (which owns its
// Store). It is safe to call more than once.
func (local *Local) Close() error {
	if local == nil {
		return nil
	}
	local.closeOnce.Do(func() {
		var closeErrors []error
		if local.cancel != nil {
			local.cancel()
		}
		if local.httpServer != nil {
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), local.shutdownTimeout)
			err := local.httpServer.Shutdown(shutdownContext)
			shutdownCancel()
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				err = local.httpServer.Close()
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				closeErrors = append(closeErrors, fmt.Errorf("close local HTTP server: %w", err))
			}
			if local.serveDone != nil {
				<-local.serveDone
			}
		} else if local.listener != nil {
			if err := local.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				closeErrors = append(closeErrors, fmt.Errorf("close local listener: %w", err))
			}
		}
		if local.callerRuntime != nil {
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), local.shutdownTimeout)
			if err := local.callerRuntime.Shutdown(shutdownContext); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close local caller transport: %w", err))
			}
			shutdownCancel()
		}
		if local.webServer != nil {
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), local.shutdownTimeout)
			if err := local.webServer.Shutdown(shutdownContext); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close local web server: %w", err))
			}
			shutdownCancel()
		}
		if local.webHTTP != nil {
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), local.shutdownTimeout)
			err := local.webHTTP.Shutdown(shutdownContext)
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				err = local.webHTTP.Close()
			}
			shutdownCancel()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				closeErrors = append(closeErrors, fmt.Errorf("close local web HTTP server: %w", err))
			}
		} else if local.webListener != nil {
			if err := local.webListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				closeErrors = append(closeErrors, fmt.Errorf("close local web listener: %w", err))
			}
		}
		if local.webWorker != nil {
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), local.shutdownTimeout)
			if err := local.webWorker.Shutdown(shutdownContext); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close local web workerkit: %w", err))
			}
			shutdownCancel()
		}
		if local.webMirror != nil {
			if err := local.webMirror.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close local web mirror: %w", err))
			}
		}
		if local.webStateRelease != nil {
			if err := local.webStateRelease(context.Background()); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("release local web worker state: %w", err))
			}
		}
		if local.service != nil {
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), local.shutdownTimeout)
			if err := local.service.Shutdown(shutdownContext); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close local llm service: %w", err))
			}
			shutdownCancel()
		}
		local.closeErr = errors.Join(closeErrors...)
	})
	return local.closeErr
}

func (local *Local) serve() {
	err := local.httpServer.Serve(local.listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		local.serveMu.Lock()
		local.serveErr = err
		local.serveMu.Unlock()
		if local.cancel != nil {
			local.cancel()
		}
	}
	close(local.serveDone)
}

func (local *Local) loadServeError() error {
	local.serveMu.Lock()
	defer local.serveMu.Unlock()
	return local.serveErr
}

func requireLoopback(address net.Addr) error {
	tcpAddress, ok := address.(*net.TCPAddr)
	if !ok || tcpAddress.IP == nil || !tcpAddress.IP.IsLoopback() {
		return fmt.Errorf("open local listener: address %q is not loopback", address.String())
	}
	return nil
}

func randomToken() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("allocate random token: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
