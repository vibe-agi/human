package human

import (
	"context"

	"github.com/vibe-agi/human/agent"
)

// AgentConfig configures the durable, task-oriented HumanAgent surface.
// DatabasePath is intentionally explicit; NewAgent never creates hidden
// process-global state.
type AgentConfig = agent.Config

// Agent owns the durable HumanAgent task lifecycle. It does not start a
// listener or a worker transport; A2A and other transports are adapters over
// this domain object.
type Agent = agent.Agent

// DefaultAgentConfig returns the durable Agent defaults without selecting a
// database identity. Set DatabasePath before calling NewAgent.
func DefaultAgentConfig() AgentConfig {
	return agent.DefaultConfig()
}

// NewAgent opens or recovers the task-oriented HumanAgent surface. ctx controls
// only this open operation; after NewAgent returns, method contexts and Close
// own the Agent lifetime. HumanAgent shares message and workspace concepts with
// HumanLLM, but not completion request, stream, or timeout lifecycle state.
func NewAgent(ctx context.Context, config AgentConfig) (*Agent, error) {
	return agent.Open(ctx, config)
}
