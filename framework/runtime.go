package framework

import "context"

// Runtime is the common lifecycle exposed by active Human compositions.
// Constructor contexts only bound initialization and recovery; after a
// successful constructor returns, Shutdown owns the runtime lifetime.
type Runtime interface {
	// Done closes after shutdown has stopped background work and released every
	// explicitly owned resource.
	Done() <-chan struct{}
	// Err returns the terminal runtime error after Done closes. A clean explicit
	// shutdown returns nil.
	Err() error
	// Shutdown stops admission, drains accepted work where the concrete runtime
	// contract requires it, and releases owned dependencies in reverse order.
	// It must be safe for concurrent and repeated calls.
	Shutdown(context.Context) error
}
