package humancmd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/internal/workerbridge"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
	workerkitsqlite "github.com/vibe-agi/human/workerkit/sqlite"
)

func sha256Sum(value string) []byte {
	sum := sha256.Sum256([]byte(value))
	return sum[:]
}

// runWebWorker connects a remote gateway over the worker bridge and serves
// the browser human side on a dedicated loopback listener until ctx ends.
func runWebWorker(
	ctx context.Context,
	gatewayURL, token, mirrorRoot, outboxPath, listenAddress string,
	mirrorScope workerkit.WorkspaceScope,
) error {
	bridge, err := workerbridge.Dial(ctx, workerbridge.Config{
		URL: gatewayURL, Token: token, OutboxPath: outboxPath,
	})
	if err != nil {
		return err
	}
	defer bridge.Close()

	// Isolate the human-side state by gateway: pointing this worker at a
	// different gateway must not recover the previous gateway's conversations
	// (whose bridge assignments no longer exist), matching the outbox's own
	// per-endpoint isolation.
	scope := hex.EncodeToString(sha256Sum(
		gatewayURL + "\x00" + string(mirrorScope.Caller) + "\x00" + mirrorScope.WorkspaceKey,
	))[:16]
	statePath, err := userdata.Path("worker", scope, "workerkit-state.db")
	if err != nil {
		return fmt.Errorf("resolve workerkit state path: %w", err)
	}
	stateResource, err := workerkitsqlite.Open(ctx, workerkitsqlite.Config{Path: statePath})
	if err != nil {
		return err
	}
	defer func() { _ = stateResource.Release(context.Background()) }()
	stateStore, err := stateResource.Value()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(mirrorRoot, 0o700); err != nil {
		return fmt.Errorf("create Human workspace base: %w", err)
	}
	mirror, err := fsmirror.Open(ctx, fsmirror.Config{
		Root:          mirrorRoot,
		Scope:         mirrorScope,
		BuildSnapshot: fsmirror.OpenCodeNativeBuilder(),
		BaselineFile:  filepath.Join(filepath.Dir(statePath), "workerkit-mirror-baseline.json"),
	})
	if err != nil {
		return err
	}
	defer mirror.Close()

	worker, err := workerkit.Open(ctx, workerkit.Config{
		Wire: bridge, State: stateStore, Mirror: mirror,
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Shutdown(shutdownContext)
	}()

	tokenBytes := make([]byte, 24)
	if _, err := rand.Read(tokenBytes); err != nil {
		return fmt.Errorf("allocate web session token: %w", err)
	}
	sessionToken := hex.EncodeToString(tokenBytes)
	server, err := web.New(web.Config{Worker: worker, SessionToken: sessionToken})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("open web listener: %w", err)
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !address.IP.IsLoopback() {
		_ = listener.Close()
		return errors.New("web listener must bind a loopback address; put TLS and SSO in front for remote access")
	}
	httpServer := &http.Server{Handler: server, ReadHeaderTimeout: 10 * time.Second}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	fmt.Printf("Human workspace base: %s\nhuman side (browser): http://%s/?token=%s\n",
		mirrorRoot, listener.Addr(), sessionToken)

	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	// Cancel the web server's pump first so SSE handlers return, then drain the
	// HTTP server; each gets a fresh context so a slow first step cannot starve
	// the second.
	serverContext, serverCancel := context.WithTimeout(context.Background(), 10*time.Second)
	serverErr := server.Shutdown(serverContext)
	serverCancel()
	httpContext, httpCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := httpServer.Shutdown(httpContext); errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		_ = httpServer.Close()
	}
	httpCancel()
	return serverErr
}
