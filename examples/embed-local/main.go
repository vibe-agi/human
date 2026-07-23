// Command embed-local starts a complete Human instance inside another Go
// process: loopback model API, SQLite service, worker, and browser human side.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/vibe-agi/human/local"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := local.DefaultConfig()
	if err != nil {
		return fmt.Errorf("load local defaults: %w", err)
	}
	instance, err := local.Open(ctx, config)
	if err != nil {
		return fmt.Errorf("open embedded Human: %w", err)
	}

	// Pass instance.BaseURL() and instance.CallerToken() directly to the model
	// client embedded in your application. The token is intentionally never
	// printed or logged by this example.
	fmt.Fprintf(os.Stderr, "Human model API listening at %s (credential retained in process)\n", instance.BaseURL())
	fmt.Fprintf(os.Stderr, "human side (browser): %s\n", instance.WebURL())

	waitDone := make(chan error, 1)
	go func() { waitDone <- instance.Wait() }()
	var runErr error
	select {
	case runErr = <-waitDone:
	case <-ctx.Done():
	}
	closeErr := instance.Close()
	return errors.Join(runErr, closeErr)
}
