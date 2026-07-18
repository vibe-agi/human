// Package gateway provides an embeddable Human Agent model gateway.
//
// Open assembles the supported SQLite store, model-compatible HTTP handlers,
// and worker WebSocket transport without owning a TCP listener. Applications
// can use Server directly as an http.Handler, or mount ModelHandler and
// WorkerHandler in their own router and authentication perimeter.
package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	internalauth "github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/dialect/anthropic"
	"github.com/vibe-agi/human/internal/completion/dialect/openai"
	"github.com/vibe-agi/human/internal/completion/dialect/responses"
	completiongateway "github.com/vibe-agi/human/internal/completion/gateway"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/ownerlock"
	"github.com/vibe-agi/human/internal/ratelimit"
	"github.com/vibe-agi/human/internal/sqlitefile"
	"github.com/vibe-agi/human/internal/store/sqlite"
	"github.com/vibe-agi/human/internal/workerproto"
	"github.com/vibe-agi/human/internal/workerws"
)

// WorkerPath is the worker WebSocket route mounted by Server.ServeHTTP.
const WorkerPath = "/internal/v1/worker/ws"

var (
	// ErrUnauthorized may be returned by a custom Authenticator when no valid
	// application principal is present on the request.
	ErrUnauthorized = errors.New("unauthorized")
	// ErrBuiltInAuthDisabled reports that token administration was requested
	// from a Server configured to use an application's custom authenticator.
	ErrBuiltInAuthDisabled = errors.New("built-in token authentication is disabled")
	// ErrCredentialMismatch reports that a token does not own the key ID an
	// application expected. Conditional revocation returns this error without
	// revoking either the supplied token or the unrelated key ID.
	ErrCredentialMismatch = errors.New("credential does not match expected key ID")
	// ErrWorkerRouteDenied is returned by a WorkerRouter when an authenticated
	// caller is not permitted to create a task on any worker. It produces a
	// finite 403 and is distinct from an offline selected worker or router
	// infrastructure failure.
	ErrWorkerRouteDenied = errors.New("worker route denied")
	// ErrDatabaseInUse prevents two gateway runtimes from recovering and serving
	// the same single-instance SQLite state through independent in-memory hubs.
	ErrDatabaseInUse = errors.New("gateway database is already held by another running instance")
)

// PrincipalType identifies which side of the Human Agent protocol a
// principal may use. Callers access model endpoints; workers access the
// private worker WebSocket endpoint.
type PrincipalType string

const (
	PrincipalCaller PrincipalType = "caller"
	PrincipalWorker PrincipalType = "worker"
)

// Principal is the authenticated identity supplied by an embedding
// application. SubjectID and a non-empty KeyID must match the durable-key
// grammar [A-Za-z0-9][A-Za-z0-9._:-]{0,127}. KeyID is an optional stable
// credential/session identifier used for admission limiting and audit
// metadata; when empty, the gateway derives one from the type and subject.
type Principal struct {
	Type      PrincipalType
	SubjectID string
	KeyID     string
}

// Authenticator lets an application use its existing cookie, JWT, mTLS, or
// upstream identity system. It receives the complete HTTP request, including
// its context. Returning an error rejects the request.
type Authenticator interface {
	AuthenticateRequest(*http.Request) (Principal, error)
}

// AuthenticatorFunc adapts a function to Authenticator.
type AuthenticatorFunc func(*http.Request) (Principal, error)

var _ Authenticator = AuthenticatorFunc(nil)

// AuthenticateRequest implements Authenticator.
func (function AuthenticatorFunc) AuthenticateRequest(request *http.Request) (Principal, error) {
	if function == nil {
		return Principal{}, ErrUnauthorized
	}
	return function(request)
}

// CapabilityTier is the caller-side capability contract presented to worker
// routing policy.
type CapabilityTier string

const (
	CapabilityChat        CapabilityTier = "chat"
	CapabilityRemoteTools CapabilityTier = "remote_tools"
	CapabilityWorkspace   CapabilityTier = "workspace"
)

// WorkerRouteRequest is the complete policy input for the first completion in
// a task. Request is a clone of the authenticated HTTP request with a fresh,
// readable body. Caller is the authenticated principal; the remaining fields
// are decoded or validated protocol identity, never unauthenticated guesses.
// Continuations and recovery do not invoke the router: they retain the
// original durable worker owner.
type WorkerRouteRequest struct {
	Request          *http.Request
	Caller           Principal
	Model            string
	CapabilityTier   CapabilityTier
	WorkspaceKey     string
	TaskID           string
	IdempotencyKey   string
	HarnessID        string
	HarnessVersion   string
	HarnessSessionID string
	WorkspaceRoot    string
	ExecAllowed      bool
}

// WorkerRouter chooses the exact worker SubjectID for a new task. Returning
// "" explicitly requests the gateway's deterministic default worker. Return
// ErrWorkerRouteDenied for a policy denial; other errors are treated as router
// failures and never fall back to another worker.
type WorkerRouter interface {
	RouteWorker(context.Context, WorkerRouteRequest) (string, error)
}

// WorkerRouterFunc adapts a function to WorkerRouter.
type WorkerRouterFunc func(context.Context, WorkerRouteRequest) (string, error)

var _ WorkerRouter = WorkerRouterFunc(nil)

// RouteWorker implements WorkerRouter.
func (function WorkerRouterFunc) RouteWorker(
	ctx context.Context,
	request WorkerRouteRequest,
) (string, error) {
	if function == nil {
		return "", ErrWorkerRouteDenied
	}
	return function(ctx, request)
}

// IssuedToken is returned once when the built-in token service creates a
// credential. Only its hash is retained in SQLite.
type IssuedToken struct {
	KeyID  string
	Secret string
}

// PreparedToken is a built-in credential that has been generated but is not
// yet accepted by the gateway. Persist it in a private durable journal before
// calling ActivateToken to make credential rotation recoverable across a
// process crash. Secret must be treated like any other plaintext API token.
type PreparedToken struct {
	KeyID     string
	Secret    string
	Type      PrincipalType
	SubjectID string
}

// RateLimitConfig controls per-credential admission. Zero fields select the
// documented defaults.
type RateLimitConfig struct {
	RatePerSecond float64
	Burst         int
	IdleTTL       time.Duration
}

// WorkerTransportConfig controls the private worker WebSocket transport. Zero
// fields select defaults.
type WorkerTransportConfig struct {
	ReadLimit    int64
	WriteTimeout time.Duration
	PingInterval time.Duration
	PingTimeout  time.Duration
}

// Config controls an embedded gateway. SQLite is deliberately the only store
// implementation exposed in this release; internal persistence contracts are
// not presented as a stable public driver API.
type Config struct {
	DatabasePath string
	Models       []string

	QueueCapacity         int
	MaxBodyBytes          int64
	MaxWorkerMessageBytes int64
	HeartbeatInterval     time.Duration
	// StreamWriteTimeout bounds one SSE write plus flush. It is reset after
	// every successful emission and never acts as a whole-stream timeout.
	StreamWriteTimeout     time.Duration
	MaxPending             time.Duration
	RateLimit              RateLimitConfig
	Worker                 WorkerTransportConfig
	AuditPayload           bool
	AuditPayloadTTL        time.Duration
	ReplayPayloadGrace     time.Duration
	RetentionSweepInterval time.Duration

	DisableCodexAutoIdempotency bool

	// Authenticator replaces built-in bearer/X-Api-Key authentication. When
	// set, Issue and Revoke return ErrBuiltInAuthDisabled.
	Authenticator Authenticator
	// WorkerRouter binds each new task to an exact worker subject using the
	// embedding application's tenant and authorization policy. The chosen
	// subject is durable task affinity across continuations and recovery.
	WorkerRouter WorkerRouter
	Logger       *slog.Logger
}

// DefaultConfig returns protocol and runtime defaults for one gateway.
// DatabasePath is deliberately empty: an embedder must choose its persistence
// identity explicitly instead of accidentally sharing a process-global user
// database with an unrelated embedded gateway. The standalone and local CLI
// compositions supply their own private OS user-data paths.
func DefaultConfig() Config {
	return Config{
		Models:                []string{"human-expert"},
		QueueCapacity:         32,
		MaxBodyBytes:          workerproto.MaxWireMessageBytes,
		MaxWorkerMessageBytes: workerproto.MaxWireMessageBytes,
		HeartbeatInterval:     15 * time.Second,
		StreamWriteTimeout:    10 * time.Second,
		MaxPending:            10 * time.Minute,
		RateLimit: RateLimitConfig{
			RatePerSecond: ratelimit.DefaultRatePerSecond,
			Burst:         ratelimit.DefaultBurst,
			IdleTTL:       ratelimit.DefaultIdleTTL,
		},
		Worker: WorkerTransportConfig{
			ReadLimit:    workerproto.MaxWireMessageBytes,
			WriteTimeout: 10 * time.Second,
			PingInterval: 30 * time.Second,
			PingTimeout:  10 * time.Second,
		},
		AuditPayloadTTL:        7 * 24 * time.Hour,
		ReplayPayloadGrace:     24 * time.Hour,
		RetentionSweepInterval: time.Hour,
		Logger:                 slog.Default(),
	}
}

func (config Config) withDefaults() (Config, error) {
	defaults := DefaultConfig()
	// The model gateway checks an assignment before persisting admission while
	// workerws applies ReadLimit when it actually transports that assignment.
	// Treat the two public knobs as one bidirectional protocol budget: when an
	// embedder specifies only one side, carry it across; when it specifies both,
	// reject disagreement instead of accepting work the worker cannot read.
	switch {
	case config.MaxWorkerMessageBytes == 0 && config.Worker.ReadLimit > 0:
		config.MaxWorkerMessageBytes = config.Worker.ReadLimit
	case config.Worker.ReadLimit == 0 && config.MaxWorkerMessageBytes > 0:
		config.Worker.ReadLimit = config.MaxWorkerMessageBytes
	}
	if strings.TrimSpace(config.DatabasePath) == "" {
		return Config{}, errors.New("gateway database path is required; embedders must choose an explicit persistence identity")
	}
	if len(config.Models) == 0 {
		config.Models = defaults.Models
	}
	if config.QueueCapacity == 0 {
		config.QueueCapacity = defaults.QueueCapacity
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = defaults.MaxBodyBytes
	}
	if config.MaxWorkerMessageBytes == 0 {
		config.MaxWorkerMessageBytes = defaults.MaxWorkerMessageBytes
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.StreamWriteTimeout == 0 {
		config.StreamWriteTimeout = defaults.StreamWriteTimeout
	}
	if config.MaxPending == 0 {
		config.MaxPending = defaults.MaxPending
	}
	if config.RateLimit.RatePerSecond == 0 {
		config.RateLimit.RatePerSecond = defaults.RateLimit.RatePerSecond
	}
	if config.RateLimit.Burst == 0 {
		config.RateLimit.Burst = defaults.RateLimit.Burst
	}
	if config.RateLimit.IdleTTL == 0 {
		config.RateLimit.IdleTTL = defaults.RateLimit.IdleTTL
	}
	if config.Worker.ReadLimit == 0 {
		config.Worker.ReadLimit = defaults.Worker.ReadLimit
	}
	if config.Worker.WriteTimeout == 0 {
		config.Worker.WriteTimeout = defaults.Worker.WriteTimeout
	}
	if config.Worker.PingInterval == 0 {
		config.Worker.PingInterval = defaults.Worker.PingInterval
	}
	if config.Worker.PingTimeout == 0 {
		config.Worker.PingTimeout = defaults.Worker.PingTimeout
	}
	if config.AuditPayloadTTL == 0 {
		config.AuditPayloadTTL = defaults.AuditPayloadTTL
	}
	if config.ReplayPayloadGrace == 0 {
		config.ReplayPayloadGrace = defaults.ReplayPayloadGrace
	}
	if config.RetentionSweepInterval == 0 {
		config.RetentionSweepInterval = defaults.RetentionSweepInterval
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
	if config.Authenticator != nil && isNilInterface(config.Authenticator) {
		return Config{}, errors.New("authenticator must not be a typed nil")
	}
	if config.WorkerRouter != nil && isNilInterface(config.WorkerRouter) {
		return Config{}, errors.New("worker router must not be a typed nil")
	}

	switch {
	case config.QueueCapacity < 0:
		return Config{}, errors.New("queue capacity must be positive")
	case config.MaxBodyBytes < 0:
		return Config{}, errors.New("max body bytes must be positive")
	case config.MaxWorkerMessageBytes < 0:
		return Config{}, errors.New("max worker message bytes must be positive")
	case config.HeartbeatInterval < 0:
		return Config{}, errors.New("heartbeat interval must be positive")
	case config.StreamWriteTimeout < 0:
		return Config{}, errors.New("stream write timeout must be positive")
	case config.MaxPending < 0:
		return Config{}, errors.New("max pending must be positive")
	case config.AuditPayloadTTL < 0:
		return Config{}, errors.New("audit payload TTL must be positive")
	case config.ReplayPayloadGrace < 0:
		return Config{}, errors.New("replay payload grace must be positive")
	case config.RetentionSweepInterval < 0:
		return Config{}, errors.New("retention sweep interval must be positive")
	case config.Worker.ReadLimit < 0:
		return Config{}, errors.New("worker read limit must be positive")
	case config.Worker.WriteTimeout < 0:
		return Config{}, errors.New("worker write timeout must be positive")
	case config.Worker.PingInterval < 0:
		return Config{}, errors.New("worker ping interval must be positive")
	case config.Worker.PingTimeout < 0:
		return Config{}, errors.New("worker ping timeout must be positive")
	case config.MaxWorkerMessageBytes > workerproto.MaxWireMessageBytes:
		return Config{}, fmt.Errorf(
			"worker wire budget %d exceeds protocol maximum %d",
			config.MaxWorkerMessageBytes, workerproto.MaxWireMessageBytes,
		)
	case config.Worker.ReadLimit > workerproto.MaxWireMessageBytes:
		return Config{}, fmt.Errorf(
			"worker read limit %d exceeds protocol maximum %d",
			config.Worker.ReadLimit, workerproto.MaxWireMessageBytes,
		)
	case config.MaxWorkerMessageBytes != config.Worker.ReadLimit:
		return Config{}, fmt.Errorf(
			"max worker message bytes (%d) must equal worker read limit (%d)",
			config.MaxWorkerMessageBytes, config.Worker.ReadLimit,
		)
	}
	return config, nil
}

// Server owns the embedded SQLite store and protocol components. It does not
// own a listener; the embedding application controls routing and shutdown of
// its HTTP server.
type Server struct {
	database   *sqlite.Store
	ownerLock  *ownerlock.Lock
	model      *completiongateway.Server
	worker     *workerws.Server
	handler    http.Handler
	tokens     *internalauth.Service
	logger     *slog.Logger
	replayTTL  time.Duration
	sweepEvery time.Duration

	cancel context.CancelFunc
	run    sync.WaitGroup
	close  sync.Once
	err    error
}

var _ http.Handler = (*Server)(nil)

// Open initializes, purges, and recovers an embedded gateway. The
// returned Server is ready to serve HTTP. Canceling ctx stops background work
// and active completion sessions; call Close after the application's HTTP
// server has stopped accepting requests.
func Open(ctx context.Context, config Config) (*Server, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	databasePath, err := resolveDatabasePath(config.DatabasePath)
	if err != nil {
		return nil, err
	}
	location, err := sqlitefile.PreparePrivate(databasePath, "gateway database")
	if err != nil {
		return nil, err
	}
	databaseOwner, err := ownerlock.Acquire(location, "gateway database")
	if err != nil {
		if errors.Is(err, ownerlock.ErrInUse) {
			return nil, fmt.Errorf("%w: %v", ErrDatabaseInUse, err)
		}
		return nil, err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner && databaseOwner != nil {
			_ = databaseOwner.Close()
		}
	}()
	database, err := sqlite.Open(ctx, location.OpenDSN())
	if err != nil {
		return nil, err
	}
	cleanupDatabase := true
	defer func() {
		if cleanupDatabase {
			_ = database.Close()
		}
	}()

	now := time.Now()
	if _, err := database.PurgeExpiredAuditPayloads(ctx, now); err != nil {
		return nil, fmt.Errorf("purge expired audit payloads: %w", err)
	}
	if _, err := database.PurgeExpiredCompletionPayloads(ctx, now.Add(-config.ReplayPayloadGrace)); err != nil {
		return nil, fmt.Errorf("purge expired completion payloads: %w", err)
	}

	var authenticator internalauth.Authenticator
	var tokens *internalauth.Service
	if config.Authenticator == nil {
		tokens = internalauth.NewService(database)
		authenticator = tokens
	} else {
		authenticator = requestAuthenticator{custom: config.Authenticator}
	}

	workerHub := hub.New(config.QueueCapacity)
	workerServer, err := workerws.New(workerws.Config{
		ReadLimit: config.Worker.ReadLimit, WriteTimeout: config.Worker.WriteTimeout,
		PingInterval: config.Worker.PingInterval, PingTimeout: config.Worker.PingTimeout,
	}, authenticator, workerHub, database)
	if err != nil {
		return nil, err
	}
	var workerRouter completiongateway.WorkerRouter
	if config.WorkerRouter != nil {
		workerRouter = publicWorkerRouter{router: config.WorkerRouter}
	}
	modelServer, err := completiongateway.NewServer(completiongateway.Config{
		Models: config.Models, MaxBodyBytes: config.MaxBodyBytes,
		MaxWorkerMessageBytes: config.MaxWorkerMessageBytes,
		HeartbeatInterval:     config.HeartbeatInterval,
		StreamWriteTimeout:    config.StreamWriteTimeout,
		MaxPending:            config.MaxPending,
		RateLimit: ratelimit.Config{
			RatePerSecond: config.RateLimit.RatePerSecond,
			Burst:         config.RateLimit.Burst,
			IdleTTL:       config.RateLimit.IdleTTL,
		},
		AuditPayload: config.AuditPayload, AuditPayloadTTL: config.AuditPayloadTTL,
		DisableCodexAutoIdempotency: config.DisableCodexAutoIdempotency,
		WorkerRouter:                workerRouter,
		Logger:                      config.Logger,
	}, database, authenticator, workerHub, adapter.NewDefaultRegistry(), map[string]dialect.Codec{
		"/v1/chat/completions": openai.New(),
		"/v1/messages":         anthropic.New(),
		"/v1/responses":        responses.New(),
	})
	if err != nil {
		return nil, err
	}

	runContext, cancel := context.WithCancel(ctx)
	if err := modelServer.Recover(runContext); err != nil {
		cancel()
		modelServer.Wait()
		return nil, fmt.Errorf("recover incomplete completion requests: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle(WorkerPath, workerServer)
	mux.Handle("/", modelServer)
	server := &Server{
		database: database, ownerLock: databaseOwner,
		model: modelServer, worker: workerServer, handler: mux,
		tokens: tokens, logger: config.Logger, replayTTL: config.ReplayPayloadGrace,
		sweepEvery: config.RetentionSweepInterval, cancel: cancel,
	}
	server.run.Add(1)
	go server.purgeLoop(runContext)
	cleanupDatabase = false
	releaseOwner = false
	return server, nil
}

// ServeHTTP serves the built-in worker route and all model routes.
func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	server.handler.ServeHTTP(response, request)
}

// ModelHandler returns the model API handler for mounting in an application
// router. It serves /livez, /readyz, the readiness-compatible /healthz alias,
// /v1/models, /v1/chat/completions, /v1/messages, and /v1/responses.
func (server *Server) ModelHandler() http.Handler { return server.model }

// WorkerHandler returns the private worker WebSocket handler. Applications may
// mount it at a custom path; ServeHTTP mounts it at /internal/v1/worker/ws.
func (server *Server) WorkerHandler() http.Handler { return server.worker }

// Issue creates a built-in caller or worker token. It is unavailable when a
// custom Authenticator owns identity.
func (server *Server) Issue(
	ctx context.Context,
	principalType PrincipalType,
	subjectID string,
) (IssuedToken, error) {
	if server.tokens == nil {
		return IssuedToken{}, ErrBuiltInAuthDisabled
	}
	if !completion.IsStableKey(subjectID) {
		return IssuedToken{}, errors.New("subject ID must be a stable key")
	}
	internalType, err := toInternalPrincipalType(principalType)
	if err != nil {
		return IssuedToken{}, err
	}
	issued, err := server.tokens.Issue(ctx, internalType, subjectID)
	if err != nil {
		return IssuedToken{}, err
	}
	return IssuedToken{KeyID: issued.KeyID, Secret: issued.Secret}, nil
}

// PrepareToken generates a built-in credential without inserting it into the
// authentication store. The returned secret cannot authenticate until the
// exact value is passed to ActivateToken.
func (server *Server) PrepareToken(
	principalType PrincipalType,
	subjectID string,
) (PreparedToken, error) {
	if server.tokens == nil {
		return PreparedToken{}, ErrBuiltInAuthDisabled
	}
	if !completion.IsStableKey(subjectID) {
		return PreparedToken{}, errors.New("subject ID must be a stable key")
	}
	internalType, err := toInternalPrincipalType(principalType)
	if err != nil {
		return PreparedToken{}, err
	}
	prepared, err := internalauth.Prepare(internalType, subjectID)
	if err != nil {
		return PreparedToken{}, err
	}
	return PreparedToken{
		KeyID: prepared.KeyID, Secret: prepared.Secret,
		Type: principalType, SubjectID: subjectID,
	}, nil
}

// ActivateToken makes an exact prepared token authenticatable. Replaying the
// same prepared value succeeds, while a conflicting key, secret, type, or
// subject fails closed.
func (server *Server) ActivateToken(ctx context.Context, prepared PreparedToken) error {
	if server.tokens == nil {
		return ErrBuiltInAuthDisabled
	}
	if !completion.IsStableKey(prepared.KeyID) || !completion.IsStableKey(prepared.SubjectID) {
		return errors.New("prepared token key and subject must be stable keys")
	}
	internalType, err := toInternalPrincipalType(prepared.Type)
	if err != nil {
		return err
	}
	return server.tokens.Activate(ctx, internalauth.PreparedToken{
		KeyID: prepared.KeyID, Secret: prepared.Secret,
		PrincipalType: internalType, SubjectID: prepared.SubjectID,
	})
}

// ValidateToken authenticates a built-in token and returns its bound
// principal. It is useful to bind persisted local credentials to an expected
// type, subject, and key ID before using them. Invalid and revoked secrets
// return ErrUnauthorized. It is unavailable when a custom Authenticator owns
// identity.
func (server *Server) ValidateToken(ctx context.Context, secret string) (Principal, error) {
	if server.tokens == nil {
		return Principal{}, ErrBuiltInAuthDisabled
	}
	principal, err := server.tokens.Authenticate(ctx, secret)
	if err != nil {
		if errors.Is(err, internalauth.ErrUnauthorized) {
			return Principal{}, ErrUnauthorized
		}
		return Principal{}, err
	}
	publicType, err := toPublicPrincipalType(principal.Type)
	if err != nil {
		return Principal{}, fmt.Errorf("validate token principal: %w", err)
	}
	if !completion.IsStableKey(principal.SubjectID) || !completion.IsStableKey(principal.KeyID) {
		return Principal{}, errors.New("validate token principal: stored subject or key ID is not a stable key")
	}
	return Principal{
		Type: publicType, SubjectID: principal.SubjectID, KeyID: principal.KeyID,
	}, nil
}

// RevokeToken conditionally revokes a built-in token only when the supplied
// secret currently authenticates as expectedKeyID. This prevents a stale or
// tampered credential file from revoking an unrelated key by ID alone.
func (server *Server) RevokeToken(ctx context.Context, secret, expectedKeyID string) error {
	if server.tokens == nil {
		return ErrBuiltInAuthDisabled
	}
	if !completion.IsStableKey(expectedKeyID) {
		return errors.New("expected key ID must be a stable key")
	}
	principal, err := server.ValidateToken(ctx, secret)
	if err != nil {
		return err
	}
	if principal.KeyID != expectedKeyID {
		return ErrCredentialMismatch
	}
	return server.tokens.Revoke(ctx, principal.KeyID)
}

// Revoke revokes a built-in token by key ID. It is unavailable when a custom
// Authenticator owns identity.
func (server *Server) Revoke(ctx context.Context, keyID string) error {
	if server.tokens == nil {
		return ErrBuiltInAuthDisabled
	}
	return server.tokens.Revoke(ctx, keyID)
}

// Close stops completion and retention work, waits for durable cleanup, and
// closes SQLite. The embedding application should stop its HTTP server before
// calling Close so no handler can race store shutdown. Close is idempotent.
func (server *Server) Close() error {
	server.close.Do(func() {
		server.cancel()
		server.model.Wait()
		server.run.Wait()
		server.err = server.database.Close()
		if server.ownerLock != nil {
			server.err = errors.Join(server.err, server.ownerLock.Close())
		}
	})
	return server.err
}

func (server *Server) purgeLoop(ctx context.Context) {
	defer server.run.Done()
	ticker := time.NewTicker(server.sweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := server.database.PurgeExpiredAuditPayloads(ctx, now); err != nil && ctx.Err() == nil {
				server.logger.Error("purge expired audit payloads", "error", err)
			}
			if _, err := server.database.PurgeExpiredCompletionPayloads(ctx, now.Add(-server.replayTTL)); err != nil && ctx.Err() == nil {
				server.logger.Error("purge expired completion payloads", "error", err)
			}
		}
	}
}

func resolveDatabasePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return path, nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create database directory: %w", err)
	}
	return absolute, nil
}

type requestAuthenticator struct {
	custom Authenticator
}

var _ internalauth.Authenticator = requestAuthenticator{}

func (authenticator requestAuthenticator) AuthenticateRequest(request *http.Request) (internalauth.Principal, error) {
	principal, err := authenticator.custom.AuthenticateRequest(request)
	if err != nil {
		return internalauth.Principal{}, err
	}
	if !completion.IsStableKey(principal.SubjectID) {
		return internalauth.Principal{}, ErrUnauthorized
	}
	internalType, err := toInternalPrincipalType(principal.Type)
	if err != nil {
		return internalauth.Principal{}, ErrUnauthorized
	}
	if principal.KeyID == "" {
		digest := sha256.Sum256([]byte(string(principal.Type) + "\x00" + principal.SubjectID))
		principal.KeyID = fmt.Sprintf("external:%x", digest[:])
	} else if !completion.IsStableKey(principal.KeyID) {
		return internalauth.Principal{}, ErrUnauthorized
	}
	return internalauth.Principal{
		Type: internalType, SubjectID: principal.SubjectID, KeyID: principal.KeyID,
	}, nil
}

type publicWorkerRouter struct {
	router WorkerRouter
}

var _ completiongateway.WorkerRouter = publicWorkerRouter{}

func (router publicWorkerRouter) RouteWorker(
	ctx context.Context,
	input completiongateway.WorkerRouteRequest,
) (string, error) {
	if router.router == nil {
		return "", nil
	}
	principalType, err := toPublicPrincipalType(input.Caller.Type)
	if err != nil {
		return "", fmt.Errorf("convert caller principal for worker routing: %w", err)
	}
	workerID, err := router.router.RouteWorker(ctx, WorkerRouteRequest{
		Request: input.HTTPRequest,
		Caller: Principal{
			Type: principalType, SubjectID: input.Caller.SubjectID, KeyID: input.Caller.KeyID,
		},
		Model:            input.Model,
		CapabilityTier:   CapabilityTier(input.Tier),
		WorkspaceKey:     input.Identity.WorkspaceKey,
		TaskID:           input.Identity.TaskID,
		IdempotencyKey:   input.Identity.IdempotencyKey,
		HarnessID:        input.Identity.HarnessID,
		HarnessVersion:   input.Identity.HarnessVersion,
		HarnessSessionID: input.Identity.HarnessSessionID,
		WorkspaceRoot:    input.Identity.Root,
		ExecAllowed:      input.Identity.ExecAllowed,
	})
	if errors.Is(err, ErrWorkerRouteDenied) {
		return "", completiongateway.ErrWorkerRouteDenied
	}
	return workerID, err
}

func toPublicPrincipalType(principalType internalauth.PrincipalType) (PrincipalType, error) {
	switch principalType {
	case internalauth.PrincipalCaller:
		return PrincipalCaller, nil
	case internalauth.PrincipalWorker:
		return PrincipalWorker, nil
	default:
		return "", errors.New("stored principal type must be caller or worker")
	}
}

func isNilInterface(value any) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func toInternalPrincipalType(principalType PrincipalType) (internalauth.PrincipalType, error) {
	switch principalType {
	case PrincipalCaller:
		return internalauth.PrincipalCaller, nil
	case PrincipalWorker:
		return internalauth.PrincipalWorker, nil
	default:
		return "", errors.New("principal type must be caller or worker")
	}
}
