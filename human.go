// Package human exposes the two top-level Human protocol surfaces.
//
// NewLLM opens the real-time, response-oriented model gateway that already
// powers the OpenAI- and Anthropic-compatible endpoints. NewAgent is a
// separate task-oriented surface; its lifecycle is intentionally not built on
// completion requests.
//
// The lower-level agent, workspace, gateway, local, and worker packages remain
// available to embedders that need domain types or direct composition of
// listeners, credentials, and the stock terminal UI.
package human

import (
	"context"

	"github.com/vibe-agi/human/gateway"
)

// LLMConfig is the configuration for the real-time HumanLLM surface.
//
// It is an alias, rather than a copied configuration, so the root facade and
// gateway package cannot drift. DatabasePath must identify explicit durable
// storage; DefaultLLMConfig deliberately leaves it empty.
type LLMConfig = gateway.Config

// LLM is the real-time HumanLLM surface. It implements http.Handler and owns
// the same durable gateway lifecycle as gateway.Server. Stop any embedding
// HTTP listener before calling Close.
type LLM = gateway.Server

// LLMWorkerPath is the built-in private worker WebSocket route used when LLM
// is mounted directly as an HTTP handler.
const LLMWorkerPath = gateway.WorkerPath

// DefaultLLMConfig returns the gateway defaults without selecting a database
// identity. The caller must set DatabasePath before NewLLM.
func DefaultLLMConfig() LLMConfig {
	return gateway.DefaultConfig()
}

// NewLLM opens and recovers the real-time HumanLLM surface. ctx controls the
// whole gateway runtime, including background work and active sessions. This
// is the conceptual root facade for gateway.Open and deliberately preserves
// its exact wire protocol, SQLite schema, handler set, and shutdown semantics.
func NewLLM(ctx context.Context, config LLMConfig) (*LLM, error) {
	return gateway.Open(ctx, config)
}
