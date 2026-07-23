package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
	"github.com/vibe-agi/human/llm/callerhttp"
	"github.com/vibe-agi/human/llm/callerhttp/bearerauth"
	"github.com/vibe-agi/human/llm/callerhttp/harnessresolver"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
)

const (
	localDeploymentID = "human-local"
)

// openPublicStackInstance composes a local instance on the public llm.Service
// stack: the caller HTTP endpoint (callerhttp with the basic harness resolver
// and bearer authenticator) and an in-process human worker sharing one
// *llm.Service. Because the worker runs in-process there is no worker token and
// nothing durable to revoke — one loopback caller token is the entire
// credential surface.
func openPublicStackInstance(ctx context.Context, config Config) (*Local, error) {
	callerID := llm.CallerID(config.CallerSubject)
	workerID := llm.WorkerID(config.WorkerSubject)
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("open local listener: %w", err)
	}
	if err := requireLoopback(listener.Addr()); err != nil {
		_ = listener.Close()
		return nil, err
	}

	runContext, cancel := context.WithCancel(ctx)
	instance := &Local{
		listener:        listener,
		baseURL:         "http://" + listener.Addr().String(),
		runContext:      runContext,
		cancel:          cancel,
		shutdownTimeout: config.ShutdownTimeout,
		serveDone:       make(chan struct{}),
	}
	fail := func(openErr error) (*Local, error) {
		if closeErr := instance.Close(); closeErr != nil {
			openErr = errors.Join(openErr, fmt.Errorf("clean up local instance: %w", closeErr))
		}
		return nil, openErr
	}

	storeResource, err := llmsqlite.Open(runContext, llmsqlite.Config{Path: config.Public.DatabasePath})
	if err != nil {
		return fail(fmt.Errorf("open local llm store: %w", err))
	}
	service, err := llm.NewService(runContext, llm.Config{
		DeploymentID: localDeploymentID,
		Store:        storeResource,
		Codecs:       builtin.Registrations(),
		Admission:    llm.AdmitAll(),
		Router: llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
			return workerID, nil
		}),
		ToolAuthorizer:   llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error { return nil }),
		AssignmentBuffer: config.Public.AssignmentBuffer,
	})
	if err != nil {
		return fail(fmt.Errorf("open local llm service: %w", err))
	}
	instance.service = service

	// A caller token that survives restarts must be supplied by the host (it is
	// what the OpenCode provider config points at); Local generates an ephemeral
	// one only when none is provided, for tests and throwaway instances.
	callerToken := config.ExistingCallerToken
	newlyIssued := false
	if callerToken == "" {
		generated, tokenErr := randomToken()
		if tokenErr != nil {
			return fail(fmt.Errorf("allocate local caller token: %w", tokenErr))
		}
		callerToken = generated
		newlyIssued = true
	}
	instance.credentials = Credentials{CallerToken: callerToken, NewlyIssued: newlyIssued}
	authenticator, err := bearerauth.New(callerToken, callerID)
	if err != nil {
		return fail(fmt.Errorf("open local authenticator: %w", err))
	}
	workspaceRoot := config.HumanWorkspaceRoot
	workspaceKey, err := userdata.WorkspaceKey(workspaceRoot)
	if err != nil {
		return fail(fmt.Errorf("resolve local workspace identity: %w", err))
	}
	resolver, err := harnessresolver.New(harnessresolver.Config{
		WorkspaceKey: workspaceKey, ExecAllowed: true,
	})
	if err != nil {
		return fail(fmt.Errorf("open local harness resolver: %w", err))
	}
	transport, err := callerhttp.New(callerhttp.Config{
		Authenticator: authenticator, Resolver: resolver, Routes: callerhttp.BuiltinRoutes(),
		WriteTimeout: config.Public.CallerWriteTimeout, HeartbeatInterval: config.Public.CallerHeartbeat,
	})
	if err != nil {
		return fail(fmt.Errorf("open local caller transport: %w", err))
	}
	runtime, err := transport.Start(runContext, service)
	if err != nil {
		return fail(fmt.Errorf("start local caller transport: %w", err))
	}
	instance.callerRuntime = runtime

	modelMux := http.NewServeMux()
	modelMux.Handle("/", transport)
	modelMux.HandleFunc("GET /readyz", instance.publicHealthHandler(service))
	modelMux.HandleFunc("GET /healthz", instance.publicHealthHandler(service))
	modelMux.HandleFunc("GET /v1/models", publicModelsHandler(authenticator))
	modelMux.Handle("POST "+callerhttp.CountTokensPath,
		publicAuthenticatedHandler(authenticator, callerhttp.CountTokensHandler()))
	instance.httpServer = &http.Server{
		Handler:           modelMux,
		BaseContext:       func(net.Listener) context.Context { return runContext },
		ReadHeaderTimeout: 10 * time.Second,
	}
	go instance.serve()
	instance.startPublicRetention(runContext, service, config.Public)
	instance.startPublicExpiry(runContext, service, config.Public)

	if err := instance.openPublicWebHumanSide(
		runContext, config, service, workerID,
		workerkit.WorkspaceScope{Caller: callerID, WorkspaceKey: workspaceKey},
	); err != nil {
		return fail(err)
	}
	select {
	case <-instance.serveDone:
		serveErr := instance.loadServeError()
		if serveErr == nil {
			serveErr = errors.New("HTTP server stopped during startup")
		}
		return fail(fmt.Errorf("open local HTTP server: %w", serveErr))
	default:
	}
	go func() {
		<-runContext.Done()
		_ = instance.Close()
	}()
	return instance, nil
}

func publicAuthenticatedHandler(authenticator callerhttp.Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if _, err := authenticator.AuthenticateCaller(request.Context(), request); err != nil {
			writeLocalJSON(response, http.StatusUnauthorized, map[string]any{
				"type": "error",
				"error": map[string]string{
					"message": "caller authentication failed", "type": "authentication_error",
				},
			})
			return
		}
		next.ServeHTTP(response, request)
	})
}

func (local *Local) publicHealthHandler(service *llm.Service) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		defer cancel()
		status, err := service.Status(ctx)
		if err != nil {
			writeLocalJSON(response, http.StatusServiceUnavailable, map[string]any{
				"status": "unavailable", "database": map[string]string{"status": "error"},
				"recovery": map[string]bool{"complete": false},
				"workers":  map[string]any{"online": 0, "has_online": false},
			})
			return
		}
		writeLocalJSON(response, http.StatusOK, map[string]any{
			"status": "ok", "database": map[string]string{"status": "ok"},
			"recovery": map[string]bool{"complete": status.RecoveryComplete},
			"workers": map[string]any{
				"online": status.WorkersOnline, "has_online": status.WorkersOnline > 0,
			},
		})
	}
}

func publicModelsHandler(authenticator callerhttp.Authenticator) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		if _, err := authenticator.AuthenticateCaller(request.Context(), request); err != nil {
			writeLocalJSON(response, http.StatusUnauthorized, map[string]any{
				"error": map[string]string{"message": "caller authentication failed", "type": "authentication_error"},
			})
			return
		}
		writeLocalJSON(response, http.StatusOK, map[string]any{
			"object": "list",
			"data":   []map[string]any{{"id": "human-expert", "object": "model", "owned_by": "human"}},
		})
	}
}

func writeLocalJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func (local *Local) startPublicRetention(ctx context.Context, service *llm.Service, config PublicStackConfig) {
	run := func() error {
		_, err := service.RunRetention(ctx, llm.RetentionPolicy{
			CompletedBefore: time.Now().UTC().Add(-config.ReplayPayloadGrace),
		})
		return err
	}
	go func() {
		if err := run(); err != nil && ctx.Err() == nil {
			local.failPublicBackground(fmt.Errorf("local retention sweep: %w", err))
			return
		}
		ticker := time.NewTicker(config.RetentionSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := run(); err != nil && ctx.Err() == nil {
					local.failPublicBackground(fmt.Errorf("local retention sweep: %w", err))
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (local *Local) startPublicExpiry(ctx context.Context, service *llm.Service, config PublicStackConfig) {
	run := func() error {
		_, err := service.RunExpiry(ctx, llm.ExpiryPolicy{
			PendingBefore: time.Now().UTC().Add(-config.MaxPending),
		})
		return err
	}
	go func() {
		if err := run(); err != nil && ctx.Err() == nil {
			local.failPublicBackground(fmt.Errorf("local expiry sweep: %w", err))
			return
		}
		ticker := time.NewTicker(config.ExpirySweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := run(); err != nil && ctx.Err() == nil {
					local.failPublicBackground(fmt.Errorf("local expiry sweep: %w", err))
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (local *Local) failPublicBackground(err error) {
	local.serveMu.Lock()
	if local.serveErr == nil {
		local.serveErr = err
	}
	local.serveMu.Unlock()
	if local.httpServer != nil {
		_ = local.httpServer.Close()
	}
}

// openPublicWebHumanSide wires the browser human side to the shared service
// in-process (service.OpenWorker + workerkit.WrapConnection), then composes the
// stock mirror, durable state, auto-title responder, and Web server.
func (local *Local) openPublicWebHumanSide(
	ctx context.Context,
	config Config,
	service *llm.Service,
	workerID llm.WorkerID,
	scope workerkit.WorkspaceScope,
) error {
	sessionID, err := randomToken()
	if err != nil {
		return fmt.Errorf("allocate local worker session id: %w", err)
	}
	connection, err := service.OpenWorker(ctx, llm.AuthenticatedWorker{
		WorkerID: workerID, SessionID: llm.WorkerSessionID(sessionID),
	})
	if err != nil {
		return fmt.Errorf("open local in-process worker: %w", err)
	}

	stateStore, baselineFile, err := local.openWebState(ctx, config)
	if err != nil {
		return err
	}
	mirrorRoot := config.HumanWorkspaceRoot
	if err := os.MkdirAll(mirrorRoot, 0o700); err != nil {
		return fmt.Errorf("create local Human workspace: %w", err)
	}
	nativeBuilders, err := fsmirror.NewNativeBuilderRegistry(
		fsmirror.NativeProfile{
			HarnessID: "opencode", HarnessVersion: "1.17.18", Build: fsmirror.OpenCodeNativeBuilder(),
		},
		fsmirror.NativeProfile{
			HarnessID: "claude-code", HarnessVersion: "2.1.217", Build: fsmirror.ClaudeCodeNativeBuilder(),
		},
		fsmirror.NativeProfile{
			HarnessID: "codex", HarnessVersion: "0.145.0", Build: fsmirror.CodexApplyPatchBuilder(),
		},
	)
	if err != nil {
		return fmt.Errorf("open local native mirror profiles: %w", err)
	}
	mirror, err := fsmirror.Open(ctx, fsmirror.Config{
		Root:          mirrorRoot,
		Scope:         scope,
		BuildSnapshot: nativeBuilders.Build,
		BaselineFile:  baselineFile,
	})
	if err != nil {
		return fmt.Errorf("open local Human workspace mirror: %w", err)
	}
	local.webMirror = mirror

	workerConfig := workerkit.Config{Wire: workerkit.WrapConnection(connection), State: stateStore, Mirror: mirror}
	if !config.WebDisableAutoTitle {
		workerConfig.AutoResponder = autoTitleResponder
	}
	webWorker, err := workerkit.Open(ctx, workerConfig)
	if err != nil {
		return fmt.Errorf("open local web workerkit: %w", err)
	}
	local.webWorker = webWorker

	sessionToken, err := randomToken()
	if err != nil {
		return fmt.Errorf("allocate local web session token: %w", err)
	}
	webServer, err := web.New(web.Config{Worker: webWorker, SessionToken: sessionToken})
	if err != nil {
		return fmt.Errorf("open local web server: %w", err)
	}
	local.webServer = webServer

	webListener, err := net.Listen("tcp", config.WebListenAddress)
	if err != nil {
		return fmt.Errorf("open local web listener: %w", err)
	}
	if err := requireLoopback(webListener.Addr()); err != nil {
		_ = webListener.Close()
		return err
	}
	local.webListener = webListener
	local.webHTTP = &http.Server{
		Handler:           webServer,
		BaseContext:       func(net.Listener) context.Context { return local.runContext },
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := local.webHTTP.Serve(webListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			local.failPublicBackground(fmt.Errorf("local web HTTP server: %w", err))
		}
	}()
	local.webURL = "http://" + webListener.Addr().String() + "/?token=" + sessionToken
	return nil
}
