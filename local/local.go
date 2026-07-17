// Package local runs a complete Human Agent instance in one process.
//
// It owns a loopback HTTP listener, an embedded SQLite-backed gateway, and a
// Human worker with its Bubble Tea model. Applications that need custom
// routing or identity should compose the gateway and worker packages directly;
// local intentionally uses built-in tokens and never persists their plaintext
// values. An embedding application may persist returned credentials itself.
package local

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/vibe-agi/human/gateway"
	"github.com/vibe-agi/human/worker"
)

const (
	// DefaultListenAddress asks the kernel for a free IPv4 loopback port.
	DefaultListenAddress = "127.0.0.1:0"
	defaultCallerSubject = "local-caller"
	defaultWorkerSubject = "local-worker"
	defaultShutdownWait  = 5 * time.Second
)

// Config controls a one-process local deployment. Gateway.Authenticator must
// be nil: local provisions built-in caller and worker tokens, reuses an
// existing pair, or obtains one from CredentialProvider so the private
// WebSocket and public model routes use the same embedded identity store.
//
// Worker.GatewayURL is replaced with the actual loopback endpoint. Worker.Token
// must be empty because local issues the credential. Library paths are passed
// through literally; shell syntax such as ~ is not expanded.
type Config struct {
	Gateway gateway.Config
	Worker  worker.Config

	ListenAddress   string
	CallerSubject   string
	WorkerSubject   string
	ShutdownTimeout time.Duration

	// ExistingCallerToken and ExistingWorkerToken reuse credentials already
	// issued into Gateway.DatabasePath. Supply both or neither. Local binds both
	// tokens to their expected principal type and configured subject before any
	// request is served; the worker then also crosses the real WebSocket
	// handshake. Existing key IDs are optional, but when known they must also be
	// supplied as a pair and match the authenticated tokens exactly.
	ExistingCallerToken string
	ExistingWorkerToken string
	ExistingCallerKeyID string
	ExistingWorkerKeyID string

	// CredentialProvider runs after the embedded gateway has recovered but
	// before HTTP serving or the worker starts. It lets an embedding application
	// complete a durable two-phase credential journal against the exact gateway
	// instance that Local will use. When set, Existing* fields must be empty and
	// the returned pair is validated against CallerSubject and WorkerSubject.
	CredentialProvider func(context.Context, *gateway.Server) (Credentials, error)
}

// Credentials are the plaintext values needed by a local caller and worker.
// NewlyIssued is true only when Open created both credentials. The library
// keeps these values in memory; an application that wants restart reuse owns
// any encrypted or mode-0600 persistence outside this package.
type Credentials struct {
	CallerToken string
	WorkerToken string
	CallerKeyID string
	WorkerKeyID string
	NewlyIssued bool
}

// DefaultConfig returns local desktop defaults. The returned worker paths are
// absolute paths below the current user's home directory.
func DefaultConfig() (Config, error) {
	workerConfig, err := worker.DefaultConfig()
	if err != nil {
		return Config{}, err
	}
	return Config{
		Gateway:         gateway.DefaultConfig(),
		Worker:          workerConfig,
		ListenAddress:   DefaultListenAddress,
		CallerSubject:   defaultCallerSubject,
		WorkerSubject:   defaultWorkerSubject,
		ShutdownTimeout: defaultShutdownWait,
	}, nil
}

func (config Config) withDefaults() (Config, error) {
	defaults, err := DefaultConfig()
	if err != nil {
		return Config{}, err
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
	if strings.TrimSpace(config.Worker.MirrorRoot) == "" {
		config.Worker.MirrorRoot = defaults.Worker.MirrorRoot
	}
	if strings.TrimSpace(config.Worker.OutboxPath) == "" {
		config.Worker.OutboxPath = defaults.Worker.OutboxPath
	}
	if !config.Worker.DisableState && strings.TrimSpace(config.Worker.StatePath) == "" {
		config.Worker.StatePath = defaults.Worker.StatePath
	}
	if strings.TrimSpace(config.Worker.Token) != "" {
		return Config{}, errors.New("open local: worker token must be empty; local provisions it")
	}
	config.ExistingCallerToken = strings.TrimSpace(config.ExistingCallerToken)
	config.ExistingWorkerToken = strings.TrimSpace(config.ExistingWorkerToken)
	config.ExistingCallerKeyID = strings.TrimSpace(config.ExistingCallerKeyID)
	config.ExistingWorkerKeyID = strings.TrimSpace(config.ExistingWorkerKeyID)
	if (config.ExistingCallerToken == "") != (config.ExistingWorkerToken == "") {
		return Config{}, errors.New("open local: existing caller and worker tokens must be supplied together")
	}
	if (config.ExistingCallerKeyID == "") != (config.ExistingWorkerKeyID == "") {
		return Config{}, errors.New("open local: existing caller and worker key IDs must be supplied together")
	}
	if config.ExistingCallerToken == "" && config.ExistingCallerKeyID != "" {
		return Config{}, errors.New("open local: existing key IDs require existing caller and worker tokens")
	}
	if config.CredentialProvider != nil && config.ExistingCallerToken != "" {
		return Config{}, errors.New("open local: credential provider and existing credentials are mutually exclusive")
	}
	if config.Gateway.Authenticator != nil {
		return Config{}, errors.New("open local: custom gateway authenticator is not supported; compose gateway and worker directly")
	}
	config.ListenAddress = strings.TrimSpace(config.ListenAddress)
	config.CallerSubject = strings.TrimSpace(config.CallerSubject)
	config.WorkerSubject = strings.TrimSpace(config.WorkerSubject)
	return config, nil
}

// Local owns one embedded gateway, loopback HTTP server, and worker. Close is
// idempotent. A Local supports one active Bubble Tea program at a time through
// Run, matching worker.Worker.
type Local struct {
	gateway *gateway.Server
	worker  *worker.Worker

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

	closeOnce        sync.Once
	closeErr         error
	revokeOnClose    bool // set only while rolling back a failed Open
	issuedDuringOpen bool // true as soon as the first built-in token is issued
}

// Open starts a complete local instance. It binds only an IP loopback
// interface, opens and recovers the gateway, issues or validates built-in
// tokens, starts HTTP serving, and connects the in-process worker to the actual
// WebSocket address. Any partial failure closes every component already opened.
func Open(ctx context.Context, config Config) (*Local, error) {
	if ctx == nil {
		return nil, errors.New("open local: context is required")
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("open local listener: %w", err)
	}
	if err := requireLoopback(listener.Addr()); err != nil {
		_ = listener.Close()
		return nil, err
	}

	runContext, cancel := context.WithCancel(ctx)
	gatewayServer, err := gateway.Open(runContext, config.Gateway)
	if err != nil {
		cancel()
		_ = listener.Close()
		return nil, fmt.Errorf("open local gateway: %w", err)
	}

	instance := &Local{
		gateway:         gatewayServer,
		listener:        listener,
		baseURL:         "http://" + listener.Addr().String(),
		runContext:      runContext,
		cancel:          cancel,
		shutdownTimeout: config.ShutdownTimeout,
		serveDone:       make(chan struct{}),
	}
	cleanupFailure := func(openErr error) (*Local, error) {
		instance.revokeOnClose = true
		if closeErr := instance.Close(); closeErr != nil {
			openErr = errors.Join(openErr, fmt.Errorf("clean up local instance: %w", closeErr))
		}
		return nil, openErr
	}

	if config.CredentialProvider != nil {
		provided, err := config.CredentialProvider(runContext, gatewayServer)
		if err != nil {
			return cleanupFailure(fmt.Errorf("provide local credentials: %w", err))
		}
		callerPrincipal, err := bindExistingCredential(
			runContext, gatewayServer, "caller", provided.CallerToken,
			gateway.PrincipalCaller, config.CallerSubject, provided.CallerKeyID,
		)
		if err != nil {
			return cleanupFailure(err)
		}
		workerPrincipal, err := bindExistingCredential(
			runContext, gatewayServer, "worker", provided.WorkerToken,
			gateway.PrincipalWorker, config.WorkerSubject, provided.WorkerKeyID,
		)
		if err != nil {
			return cleanupFailure(err)
		}
		instance.credentials = Credentials{
			CallerToken: provided.CallerToken, WorkerToken: provided.WorkerToken,
			CallerKeyID: callerPrincipal.KeyID, WorkerKeyID: workerPrincipal.KeyID,
		}
	} else if config.ExistingCallerToken == "" {
		callerToken, err := gatewayServer.Issue(runContext, gateway.PrincipalCaller, config.CallerSubject)
		if err != nil {
			return cleanupFailure(fmt.Errorf("issue local caller token: %w", err))
		}
		instance.credentials.CallerToken = callerToken.Secret
		instance.credentials.CallerKeyID = callerToken.KeyID
		// Set this before issuing the worker token. If that second Issue fails,
		// rollback must still revoke the already-created caller credential.
		instance.issuedDuringOpen = true

		workerToken, err := gatewayServer.Issue(runContext, gateway.PrincipalWorker, config.WorkerSubject)
		if err != nil {
			return cleanupFailure(fmt.Errorf("issue local worker token: %w", err))
		}
		instance.credentials.WorkerToken = workerToken.Secret
		instance.credentials.WorkerKeyID = workerToken.KeyID
		instance.credentials.NewlyIssued = true
	} else {
		callerPrincipal, err := bindExistingCredential(
			runContext, gatewayServer, "caller", config.ExistingCallerToken,
			gateway.PrincipalCaller, config.CallerSubject, config.ExistingCallerKeyID,
		)
		if err != nil {
			return cleanupFailure(err)
		}
		workerPrincipal, err := bindExistingCredential(
			runContext, gatewayServer, "worker", config.ExistingWorkerToken,
			gateway.PrincipalWorker, config.WorkerSubject, config.ExistingWorkerKeyID,
		)
		if err != nil {
			return cleanupFailure(err)
		}
		instance.credentials = Credentials{
			CallerToken: config.ExistingCallerToken,
			WorkerToken: config.ExistingWorkerToken,
			CallerKeyID: callerPrincipal.KeyID,
			WorkerKeyID: workerPrincipal.KeyID,
		}
	}

	instance.httpServer = &http.Server{
		Handler:           gatewayServer,
		BaseContext:       func(net.Listener) context.Context { return runContext },
		ReadHeaderTimeout: 10 * time.Second,
	}
	go instance.serve()

	config.Worker.GatewayURL = "ws://" + listener.Addr().String() + gateway.WorkerPath
	config.Worker.Token = instance.credentials.WorkerToken
	openedWorker, err := worker.Open(runContext, config.Worker)
	if err != nil {
		return cleanupFailure(fmt.Errorf("open local worker: %w", err))
	}
	instance.worker = openedWorker

	select {
	case <-instance.serveDone:
		serveErr := instance.loadServeError()
		if serveErr == nil {
			serveErr = errors.New("HTTP server stopped during startup")
		}
		return cleanupFailure(fmt.Errorf("open local HTTP server: %w", serveErr))
	default:
	}
	go func() {
		<-runContext.Done()
		_ = instance.Close()
	}()
	return instance, nil
}

// BaseURL returns the loopback model API base URL using the kernel-selected
// port when ListenAddress ended in :0.
func (local *Local) BaseURL() string {
	if local == nil {
		return ""
	}
	return local.baseURL
}

// CallerToken returns the configured or newly issued plaintext bearer token
// for local model API clients. Only its hash is retained by the gateway.
func (local *Local) CallerToken() string {
	if local == nil {
		return ""
	}
	return local.credentials.CallerToken
}

// Credentials returns a copy of the local caller and worker credentials and
// whether Open issued them. Treat both token fields as secrets.
func (local *Local) Credentials() Credentials {
	if local == nil {
		return Credentials{}
	}
	return local.credentials
}

// Gateway returns the embedded gateway for applications that need its
// separately mountable handlers or token administration API.
func (local *Local) Gateway() *gateway.Server {
	if local == nil {
		return nil
	}
	return local.gateway
}

// Worker returns the in-process Human worker.
func (local *Local) Worker() *worker.Worker {
	if local == nil {
		return nil
	}
	return local.worker
}

// Model returns the worker's Bubble Tea model.
func (local *Local) Model() tea.Model {
	if local == nil || local.worker == nil {
		return nil
	}
	return local.worker.Model()
}

// Run starts the stock worker TUI. It stops when either ctx or the Local
// lifecycle ends.
func (local *Local) Run(ctx context.Context, options ...tea.ProgramOption) (tea.Model, error) {
	if local == nil || local.worker == nil {
		return nil, errors.New("run local: local instance is not open")
	}
	if ctx == nil {
		return nil, errors.New("run local: context is required")
	}
	runContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(local.runContext, cancel)
	defer func() {
		stop()
		cancel()
	}()
	return local.worker.Run(runContext, options...)
}

// Wait waits for the embedded HTTP server to stop and reports an unexpected
// serving error. A normal Close returns nil.
func (local *Local) Wait() error {
	if local == nil || local.serveDone == nil {
		return nil
	}
	<-local.serveDone
	return local.loadServeError()
}

// Close stops accepting HTTP requests, closes the worker, and finally closes
// the gateway and SQLite. Issued credentials remain valid in the database so
// an application may reuse them on restart; use Gateway().Revoke to retire
// credentials deliberately. Close waits for the HTTP serving goroutine and is
// safe to call more than once.
func (local *Local) Close() error {
	if local == nil {
		return nil
	}
	local.closeOnce.Do(func() {
		var closeErrors []error
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
		if local.worker != nil {
			if err := local.worker.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close local worker: %w", err))
			}
		}
		if local.gateway != nil {
			if local.revokeOnClose {
				if err := local.revokeNewCredentials(); err != nil {
					closeErrors = append(closeErrors, fmt.Errorf("revoke failed local credentials: %w", err))
				}
			}
			if err := local.gateway.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close local gateway: %w", err))
			}
		}
		if local.cancel != nil {
			local.cancel()
		}
		local.closeErr = errors.Join(closeErrors...)
	})
	return local.closeErr
}

func bindExistingCredential(
	ctx context.Context,
	server *gateway.Server,
	label, secret string,
	wantType gateway.PrincipalType,
	wantSubject, wantKeyID string,
) (gateway.Principal, error) {
	principal, err := server.ValidateToken(ctx, secret)
	if err != nil {
		return gateway.Principal{}, fmt.Errorf("validate existing local %s token: %w", label, err)
	}
	if principal.Type != wantType {
		return gateway.Principal{}, fmt.Errorf(
			"validate existing local %s token: principal type %q does not match %q",
			label, principal.Type, wantType,
		)
	}
	if principal.SubjectID != wantSubject {
		return gateway.Principal{}, fmt.Errorf(
			"validate existing local %s token: subject %q does not match configured subject %q",
			label, principal.SubjectID, wantSubject,
		)
	}
	if wantKeyID != "" && principal.KeyID != wantKeyID {
		return gateway.Principal{}, fmt.Errorf(
			"validate existing local %s token: key ID does not match the persisted key ID",
			label,
		)
	}
	return principal, nil
}

func (local *Local) revokeNewCredentials() error {
	if local == nil || local.gateway == nil || !local.issuedDuringOpen {
		return nil
	}
	revokeContext, cancel := context.WithTimeout(context.Background(), local.shutdownTimeout)
	defer cancel()
	var revokeErrors []error
	for _, keyID := range []string{local.credentials.WorkerKeyID, local.credentials.CallerKeyID} {
		if keyID == "" {
			continue
		}
		if err := local.gateway.Revoke(revokeContext, keyID); err != nil {
			revokeErrors = append(revokeErrors, err)
		}
	}
	return errors.Join(revokeErrors...)
}

func (local *Local) serve() {
	err := local.httpServer.Serve(local.listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		local.serveMu.Lock()
		local.serveErr = err
		local.serveMu.Unlock()
		local.cancel()
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
