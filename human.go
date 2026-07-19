// Package human exposes the two top-level Human framework surfaces.
//
// NewLLM constructs the real-time, response-oriented HumanLLM correctness
// core. NewAgent constructs the durable, task-oriented HumanAgent core. Both
// are transport-neutral library objects: listeners, authentication, storage,
// encryption, worker clients, and user interfaces are explicit adapters owned
// by the embedding application.
package human

import (
	"context"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
)

// LLMConfig composes the real-time HumanLLM core. Store and DeploymentID are
// deliberately required deployment choices. Every other port can be replaced
// independently; framework.Resource records whether Human owns or merely
// borrows each stateful dependency.
type LLMConfig = llm.Config

// LLM is the transport-neutral HumanLLM correctness core. It owns no HTTP
// listener, authentication system, WebSocket endpoint, terminal UI, or hidden
// process-global state. Caller and worker transports borrow it through the
// public llm.CallerEndpoint and llm.WorkerEndpoint ports.
type LLM = llm.Service

// DefaultLLMConfig returns the built-in OpenAI Chat Completions, OpenAI
// Responses, and Anthropic Messages codecs together with the explicit
// llm.AdmitAll admission policy. It intentionally leaves Store and
// DeploymentID unset so an embedder must choose a durable identity explicitly;
// replace Admission when the deployment needs policy beyond transport
// authentication. The returned values are fresh and may be replaced or
// extended.
func DefaultLLMConfig() LLMConfig {
	return LLMConfig{Codecs: builtin.Registrations(), Admission: llm.AdmitAll()}
}

// NewLLM validates the injected contracts, recovers durable in-flight work,
// and starts the transport-neutral HumanLLM core. ctx bounds construction and
// recovery only; method contexts and Shutdown control its subsequent lifetime.
func NewLLM(ctx context.Context, config LLMConfig) (*LLM, error) {
	return llm.NewService(ctx, config)
}
