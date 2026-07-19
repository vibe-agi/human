// Package framework contains the small cross-surface contracts used to compose
// HumanLLM and HumanAgent without hiding resource ownership.
package framework

import (
	"context"
	"errors"
	"sync"
)

// ErrResourceReleased is returned when a composition attempts to consume an
// owned Resource after its release callback has started. Borrowed resources are
// never released by Resource.
var ErrResourceReleased = errors.New("framework resource is released")

// ReleaseFunc closes a resource that was explicitly transferred to a Human
// composition. The context bounds shutdown; implementations should make the
// operation idempotent because a constructor-failure cleanup and normal
// shutdown may race.
type ReleaseFunc func(context.Context) error

// Resource pairs a dependency with an explicit ownership decision. Borrowed
// dependencies remain owned by the caller. Owned dependencies are released at
// most once, including when Resource values are copied.
//
// A Resource is a composition value, not a lease. Calling Release on an owned
// Resource permanently releases that composition's dependency; Value then
// returns ErrResourceReleased. The zero Resource is borrowed and contains the
// zero value of T, which consuming constructors normally reject.
type Resource[T any] struct {
	value T
	owned *ownedResource
}

type ownedResource struct {
	mu        sync.RWMutex
	releasing bool
	released  bool
	once      sync.Once
	release   ReleaseFunc
	err       error
}

// Borrow declares that value remains caller-owned. Release is a no-op.
func Borrow[T any](value T) Resource[T] {
	return Resource[T]{value: value}
}

// Own transfers value and its explicit release callback to the receiving
// composition. Own rejects a nil callback rather than guessing ownership from
// whether value happens to implement io.Closer.
func Own[T any](value T, release ReleaseFunc) (Resource[T], error) {
	if release == nil {
		return Resource[T]{}, errors.New("framework owned resource requires a release callback")
	}
	return Resource[T]{
		value: value,
		owned: &ownedResource{release: release},
	}, nil
}

// Value returns the dependency while it has not begun release. Borrowed values
// are always returned. Consumers should call Value during construction and
// retain the returned dependency only while they own the Resource lifecycle.
func (resource Resource[T]) Value() (T, error) {
	if resource.owned == nil {
		return resource.value, nil
	}
	resource.owned.mu.RLock()
	defer resource.owned.mu.RUnlock()
	if resource.owned.releasing || resource.owned.released {
		var zero T
		return zero, ErrResourceReleased
	}
	return resource.value, nil
}

// Owned reports whether this composition must release the dependency.
func (resource Resource[T]) Owned() bool {
	return resource.owned != nil
}

// Release invokes an owned dependency's callback at most once. Concurrent
// callers observe the same result. It never closes a borrowed dependency.
func (resource Resource[T]) Release(ctx context.Context) error {
	if resource.owned == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("framework resource release requires a context")
	}
	resource.owned.once.Do(func() {
		resource.owned.mu.Lock()
		resource.owned.releasing = true
		resource.owned.mu.Unlock()

		resource.owned.err = resource.owned.release(ctx)

		resource.owned.mu.Lock()
		resource.owned.releasing = false
		resource.owned.released = true
		resource.owned.mu.Unlock()
	})
	return resource.owned.err
}
