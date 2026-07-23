package llm

import (
	"context"
	"fmt"
)

// ServiceStatus is the transport-neutral readiness projection used by hosts to
// build HTTP probes, metrics, or native health APIs. It contains no database
// implementation detail and does not prescribe an endpoint shape.
type ServiceStatus struct {
	DatabaseOK       bool
	RecoveryComplete bool
	WorkersOnline    int
}

// Status verifies that the Store still accepts a serializable read and returns
// the current worker-session count. NewService completes recovery before it
// returns, so every successful status has RecoveryComplete=true.
func (service *Service) Status(ctx context.Context) (ServiceStatus, error) {
	end, err := service.beginOperation()
	if err != nil {
		return ServiceStatus{}, err
	}
	defer end()
	if ctx == nil {
		return ServiceStatus{}, fmt.Errorf("%w: context is required", ErrInvalidServiceConfig)
	}
	if err := ctx.Err(); err != nil {
		return ServiceStatus{}, err
	}
	if err := service.store.View(ctx, func(StoreView) error { return nil }); err != nil {
		return ServiceStatus{}, err
	}
	service.mu.Lock()
	workers := len(service.connections)
	service.mu.Unlock()
	return ServiceStatus{DatabaseOK: true, RecoveryComplete: true, WorkersOnline: workers}, nil
}
