package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/protect"
	protectaead "github.com/vibe-agi/human/protect/aead"
)

func TestCustomStoreOwnership(t *testing.T) {
	base, _ := openTestAgent(t)

	borrowedConfig := DefaultConfig()
	borrowedConfig.Store = framework.Borrow[Store](base.store)
	borrowed, err := New(t.Context(), borrowedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := borrowed.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := base.store.View(t.Context(), func(StoreView) error { return nil }); err != nil {
		t.Fatalf("borrowed Store was released: %v", err)
	}

	var releases atomic.Int32
	ownedResource, err := framework.Own[Store](base.store, func(context.Context) error {
		releases.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ownedConfig := DefaultConfig()
	ownedConfig.Store = ownedResource
	owned, err := New(t.Context(), ownedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := owned.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := owned.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("owned Store releases = %d, want 1", got)
	}
	select {
	case <-owned.Done():
	default:
		t.Fatal("Done did not close after owned Store release")
	}
	if err := owned.Err(); err != nil {
		t.Fatalf("Err after clean shutdown = %v", err)
	}
}

type blockingStore struct {
	Store
	entered chan struct{}
	release chan struct{}
}

func (store *blockingStore) View(ctx context.Context, callback func(StoreView) error) error {
	close(store.entered)
	select {
	case <-store.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return store.Store.View(ctx, callback)
}

func TestShutdownDeadlineLeavesDrainingRuntimeFinishable(t *testing.T) {
	base, _ := openTestAgent(t)
	blocked := &blockingStore{
		Store: base.store, entered: make(chan struct{}), release: make(chan struct{}),
	}
	var releases atomic.Int32
	resource, err := framework.Own[Store](blocked, func(context.Context) error {
		releases.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Store = resource
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}

	operationDone := make(chan error, 1)
	go func() {
		_, err := service.GetTask(context.Background(), TaskRef{
			Workspace: WorkspaceRef{Authority: "authority", ID: "workspace"}, ID: "task",
		})
		operationDone <- err
	}()
	<-blocked.entered

	shutdownCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := service.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown while operation active = %v, want deadline", err)
	}
	if got := releases.Load(); got != 0 {
		t.Fatalf("Store released while operation active: %d", got)
	}
	close(blocked.release)
	if err := <-operationDone; !errors.Is(err, ErrNotFound) {
		t.Fatalf("blocked operation = %v, want not found", err)
	}
	if err := service.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("Store releases after drain = %d, want 1", got)
	}
	valid := TaskRef{Workspace: WorkspaceRef{Authority: "authority", ID: "workspace"}, ID: "task"}
	if _, err := service.GetTask(t.Context(), valid); !errors.Is(err, ErrClosed) {
		t.Fatalf("operation after Shutdown = %v, want ErrClosed", err)
	}
}

func TestShutdownUsesIndependentBoundedDependencyReleaseContexts(t *testing.T) {
	base, _ := openTestAgent(t)
	protectorResource, err := protectaead.Open(t.Context(), keyringConfig("v1", testKey('z')))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = protectorResource.Release(context.Background()) })
	protector, err := protectorResource.Value()
	if err != nil {
		t.Fatal(err)
	}
	ownedProtector, err := framework.Own[protect.Protector](protector, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	var storeReleases atomic.Int32
	ownedStore, err := framework.Own[Store](base.store, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		storeReleases.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Store = ownedStore
	config.Protector = ownedProtector
	config.ReleaseTimeout = 20 * time.Millisecond
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	if err := service.Shutdown(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Shutdown wait = %v, want deadline", err)
	}
	select {
	case <-service.Done():
	case <-time.After(time.Second):
		t.Fatal("background dependency release did not reach Done")
	}
	if got := storeReleases.Load(); got != 1 {
		t.Fatalf("Store releases after Protector timeout = %d, want 1", got)
	}
	if err := service.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("terminal release error = %v, want Protector deadline", err)
	}
}

type invalidDescriptionStore struct{ Store }

func (*invalidDescriptionStore) Description() StoreDescription { return StoreDescription{} }

func TestInvalidOwnedStoreIsReleasedDuringConstruction(t *testing.T) {
	base, _ := openTestAgent(t)
	var releases atomic.Int32
	resource, err := framework.Own[Store](&invalidDescriptionStore{Store: base.store}, func(context.Context) error {
		releases.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Store = resource
	if _, err := New(t.Context(), config); !errors.Is(err, ErrStoreContractMismatch) {
		t.Fatalf("New with invalid Store contract = %v", err)
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("constructor failure releases = %d, want 1", got)
	}
}

var _ framework.Runtime = (*Agent)(nil)
