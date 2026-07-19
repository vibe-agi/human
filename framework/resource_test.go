package framework

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBorrowNeverReleases(t *testing.T) {
	resource := Borrow("borrowed")
	if resource.Owned() {
		t.Fatal("borrowed resource reports owned")
	}
	if err := resource.Release(context.Background()); err != nil {
		t.Fatalf("release borrowed resource: %v", err)
	}
	value, err := resource.Value()
	if err != nil || value != "borrowed" {
		t.Fatalf("borrowed value = %q, %v", value, err)
	}
}

func TestOwnedCopiesReleaseExactlyOnce(t *testing.T) {
	var calls atomic.Int32
	want := errors.New("close result")
	resource, err := Own("owned", func(context.Context) error {
		calls.Add(1)
		return want
	})
	if err != nil {
		t.Fatalf("own resource: %v", err)
	}
	copy := resource
	var wait sync.WaitGroup
	results := make(chan error, 2)
	for _, candidate := range []Resource[string]{resource, copy} {
		wait.Add(1)
		go func(candidate Resource[string]) {
			defer wait.Done()
			results <- candidate.Release(context.Background())
		}(candidate)
	}
	wait.Wait()
	close(results)
	for result := range results {
		if !errors.Is(result, want) {
			t.Fatalf("release result = %v, want %v", result, want)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("release calls = %d, want 1", calls.Load())
	}
	if _, err := resource.Value(); !errors.Is(err, ErrResourceReleased) {
		t.Fatalf("value after release error = %v", err)
	}
}

func TestOwnRequiresExplicitCallback(t *testing.T) {
	if _, err := Own("value", nil); err == nil {
		t.Fatal("Own accepted nil release callback")
	}
}

func TestReleaseRejectsNilContextBeforeConsumingResource(t *testing.T) {
	var calls atomic.Int32
	resource, err := Own("owned", func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := resource.Release(nil); err == nil {
		t.Fatal("Release accepted nil context")
	}
	if calls.Load() != 0 {
		t.Fatal("nil context consumed release callback")
	}
	if err := resource.Release(context.Background()); err != nil {
		t.Fatalf("release after nil context: %v", err)
	}
}
