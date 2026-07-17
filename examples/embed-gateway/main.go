// Command embed-gateway demonstrates mounting Human's model and worker
// handlers in an application's HTTP server with application-owned identity and
// tenant-to-worker routing.
//
// The trusted-header authenticator in this example is deliberately NOT a
// production authentication scheme. It is only the boundary behind a trusted
// same-host reverse proxy. A production deployment must strip client-supplied
// identity headers and authenticate the proxy hop with mTLS, a verified
// signature, or an equivalent mechanism before these handlers are reachable.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/vibe-agi/human/gateway"
)

const (
	headerVerified      = "X-Example-Proxy-Verified"
	headerPrincipalType = "X-Example-Principal-Type"
	headerSubjectID     = "X-Example-Subject-Id"
	headerKeyID         = "X-Example-Credential-Id"
	headerTenantID      = "X-Example-Tenant-Id"
)

var stableKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type trustedProxyHeaders struct{}

func (trustedProxyHeaders) AuthenticateRequest(request *http.Request) (gateway.Principal, error) {
	// A marker header is not proof of authentication. This loopback check and
	// marker merely make the trust assumption visible in a runnable example.
	// Production code must cryptographically verify the proxy hop and ensure
	// untrusted clients cannot reach this listener or inject these headers.
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil || !net.ParseIP(host).IsLoopback() || request.Header.Get(headerVerified) != "1" {
		return gateway.Principal{}, gateway.ErrUnauthorized
	}

	principalType := gateway.PrincipalType(strings.TrimSpace(request.Header.Get(headerPrincipalType)))
	if principalType != gateway.PrincipalCaller && principalType != gateway.PrincipalWorker {
		return gateway.Principal{}, gateway.ErrUnauthorized
	}
	subjectID := strings.TrimSpace(request.Header.Get(headerSubjectID))
	keyID := strings.TrimSpace(request.Header.Get(headerKeyID))
	if !stableKey.MatchString(subjectID) || !stableKey.MatchString(keyID) {
		return gateway.Principal{}, gateway.ErrUnauthorized
	}
	return gateway.Principal{Type: principalType, SubjectID: subjectID, KeyID: keyID}, nil
}

type tenantRoute struct {
	callerSubject string
	workerSubject string
}

type tenantRouter map[string]tenantRoute

func (routes tenantRouter) RouteWorker(_ context.Context, request gateway.WorkerRouteRequest) (string, error) {
	tenantID := strings.TrimSpace(request.Request.Header.Get(headerTenantID))
	route, ok := routes[tenantID]
	if !ok || request.Caller.Type != gateway.PrincipalCaller || request.Caller.SubjectID != route.callerSubject {
		return "", gateway.ErrWorkerRouteDenied
	}
	return route.workerSubject, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	signalContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	databasePath, err := exampleDatabasePath()
	if err != nil {
		return err
	}
	config := gateway.DefaultConfig()
	config.DatabasePath = databasePath
	config.Authenticator = trustedProxyHeaders{}
	config.WorkerRouter = tenantRouter{
		"tenant-a": {callerSubject: "tenant-a-user", workerSubject: "tenant-a-expert"},
	}

	// Keep the gateway lifecycle independent from the signal context so the HTTP
	// server can stop accepting traffic before SQLite is closed.
	gatewayContext, cancelGateway := context.WithCancel(context.Background())
	defer cancelGateway()
	humanGateway, err := gateway.Open(gatewayContext, config)
	if err != nil {
		return fmt.Errorf("open embedded gateway: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/human/", http.StripPrefix("/human", humanGateway.ModelHandler()))
	mux.Handle("/human-worker", humanGateway.WorkerHandler())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = humanGateway.Close()
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return gatewayContext },
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()

	fmt.Fprintf(os.Stderr, "example gateway listening at http://%s/human (trusted-proxy headers are DEMO ONLY)\n", listener.Addr())

	var serveErr error
	serveStopped := false
	select {
	case <-signalContext.Done():
	case serveErr = <-serveResult:
		serveStopped = true
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := server.Shutdown(shutdownContext)
	cancelShutdown()
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	if !serveStopped {
		stoppedErr := <-serveResult
		if !errors.Is(stoppedErr, http.ErrServerClosed) {
			serveErr = errors.Join(serveErr, stoppedErr)
		}
	}
	cancelGateway()
	closeErr := humanGateway.Close()
	return errors.Join(serveErr, shutdownErr, closeErr)
}

func exampleDatabasePath() (string, error) {
	if override := strings.TrimSpace(os.Getenv("HUMAN_EMBED_GATEWAY_DB")); override != "" {
		if !filepath.IsAbs(override) {
			return "", errors.New("HUMAN_EMBED_GATEWAY_DB must be an absolute path")
		}
		return filepath.Clean(override), nil
	}
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve OS user-data directory: %w", err)
	}
	return filepath.Join(root, "human", "examples", "embed-gateway", "gateway.db"), nil
}
