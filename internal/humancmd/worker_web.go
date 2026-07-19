package humancmd

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
	"time"

	"github.com/vibe-agi/human/internal/userdata"
	"github.com/vibe-agi/human/internal/workerbridge"
	"github.com/vibe-agi/human/web"
	"github.com/vibe-agi/human/workerkit"
	"github.com/vibe-agi/human/workerkit/fsmirror"
	workerkitsqlite "github.com/vibe-agi/human/workerkit/sqlite"
)

// runWebWorker connects a remote gateway over the worker bridge and serves
// the browser human side on a dedicated loopback listener until ctx ends.
func runWebWorker(ctx context.Context, gatewayURL, token, mirrorRoot, outboxPath, listenAddress string) error {
	bridge, err := workerbridge.Dial(ctx, workerbridge.Config{
		URL: gatewayURL, Token: token, OutboxPath: outboxPath,
	})
	if err != nil {
		return err
	}
	defer bridge.Close()

	statePath, err := userdata.Path("worker", "workerkit-state.db")
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

	webMirrorRoot := filepath.Join(mirrorRoot, "web")
	if err := os.MkdirAll(webMirrorRoot, 0o700); err != nil {
		return fmt.Errorf("create web mirror root: %w", err)
	}
	mirror, err := fsmirror.Open(ctx, fsmirror.Config{
		Root:         webMirrorRoot,
		Build:        fsmirror.OpenCodeWriteBuilder(),
		BaselineFile: filepath.Join(filepath.Dir(statePath), "workerkit-mirror-baseline.json"),
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
	fmt.Printf("human side (browser): http://%s/?token=%s\n", listener.Addr(), sessionToken)

	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownContext)
	return server.Shutdown(shutdownContext)
}
