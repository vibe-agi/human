package human

import (
	"context"

	"github.com/vibe-agi/human/agent"
)

// AgentConfig configures the durable, task-oriented HumanAgent surface. Store
// is required and may be any implementation of agent.Store. NewAgent never
// chooses a physical driver or creates hidden process-global state.
type AgentConfig = agent.Config

// Agent owns the durable HumanAgent task lifecycle. It does not start a
// listener or a worker transport; A2A and other transports are adapters over
// this domain object.
type Agent = agent.Agent

// DefaultAgentConfig returns the durable Agent defaults without selecting a
// persistence identity. Set Store before calling NewAgent.
func DefaultAgentConfig() AgentConfig {
	return agent.DefaultConfig()
}

// NewAgent opens or recovers the task-oriented HumanAgent surface. ctx controls
// only this open operation; after NewAgent returns, method contexts and
// Shutdown own the Agent lifetime. HumanAgent shares message and workspace
// concepts with HumanLLM, but not completion request, stream, or timeout state.
func NewAgent(ctx context.Context, config AgentConfig) (*Agent, error) {
	return agent.Open(ctx, config)
}
