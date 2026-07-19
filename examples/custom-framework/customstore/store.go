// Package customstore demonstrates two application-owned llm.Store adapters:
// a durable single-file Store opened with [Open], and an optional policy
// middleware opened with [Own] or [Borrow]. Neither imports Human internals.
package customstore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

const cleanupTimeout = 5 * time.Second

// AuditFunc receives only a fixed operation name: bind, view, or update. It
// must be concurrency-safe, bounded, and must not panic. Customer payloads and
// persistence identities are deliberately absent.
type AuditFunc func(operation string)

// Config identifies this application adapter. Path and MaxSnapshotBytes apply
// only to Open; Own and Borrow ignore both physical-storage fields.
type Config struct {
	Path             string
	Provider         string
	Version          string
	Audit            AuditFunc
	MaxSnapshotBytes int64
}

type store struct {
	next        llm.Store
	description llm.StoreDescription
	audit       AuditFunc
}

var _ llm.Store = (*store)(nil)

// Own consumes next even when construction fails. Releasing the returned
// Resource releases the wrapped resource exactly once; the host must not also
// release next.
func Own(
	ctx context.Context,
	next framework.Resource[llm.Store],
	config Config,
) (framework.Resource[llm.Store], error) {
	if !next.Owned() {
		return framework.Resource[llm.Store]{}, errors.New(
			"custom Store: Own requires an owned wrapped Resource; use Borrow for host-owned stores",
		)
	}
	value, err := next.Value()
	if err != nil {
		return framework.Resource[llm.Store]{}, errors.Join(
			fmt.Errorf("custom Store: acquire wrapped Store: %w", err),
			release(next),
		)
	}
	wrapped, err := wrap(ctx, value, config)
	if err != nil {
		return framework.Resource[llm.Store]{}, errors.Join(err, release(next))
	}
	resource, err := framework.Own[llm.Store](wrapped, next.Release)
	if err != nil {
		return framework.Resource[llm.Store]{}, errors.Join(err, release(next))
	}
	return resource, nil
}

// Borrow decorates a host-owned Store. Releasing the returned Resource is a
// no-op; the host must keep next alive until every Human runtime using it has
// reached Done.
func Borrow(
	ctx context.Context,
	next llm.Store,
	config Config,
) (framework.Resource[llm.Store], error) {
	wrapped, err := wrap(ctx, next, config)
	if err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	return framework.Borrow[llm.Store](wrapped), nil
}

func wrap(ctx context.Context, next llm.Store, config Config) (*store, error) {
	if ctx == nil {
		return nil, errors.New("custom Store: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nilInterface(next) {
		return nil, errors.New("custom Store: wrapped Store is required")
	}
	provided := next.Description()
	if err := provided.Validate(); err != nil {
		return nil, err
	}
	contract, err := framework.Negotiate(provided.Contract, llm.StoreRequirements())
	if err != nil {
		return nil, err
	}
	description := llm.StoreDescription{
		Contract: contract,
		Provider: config.Provider,
		Version:  config.Version,
	}
	if err := description.Validate(); err != nil {
		return nil, err
	}
	return &store{next: next, description: description, audit: config.Audit}, nil
}

func (store *store) Description() llm.StoreDescription {
	description := store.description
	description.Contract.Features = maps.Clone(description.Contract.Features)
	return description
}

func (store *store) Bind(ctx context.Context, binding llm.StoreBinding) error {
	store.record("bind")
	return store.next.Bind(ctx, binding)
}

func (store *store) View(ctx context.Context, view func(llm.StoreView) error) error {
	store.record("view")
	return store.next.View(ctx, view)
}

func (store *store) Update(ctx context.Context, update func(llm.StoreTx) error) error {
	store.record("update")
	return store.next.Update(ctx, update)
}

func (store *store) record(operation string) {
	if store.audit != nil {
		store.audit(operation)
	}
}

func release(resource framework.Resource[llm.Store]) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	return resource.Release(ctx)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
