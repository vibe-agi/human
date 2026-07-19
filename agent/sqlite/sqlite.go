package sqlite

import (
	"context"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

// Config selects one dedicated HumanAgent SQLite persistence identity.
// Path may be an ordinary filesystem path or an independent :memory: database.
// Shared-memory SQLite DSNs are rejected because they cannot provide the
// adapter's single-owner lifecycle guarantee.
type Config struct {
	Path string
}

// Open constructs an owned SQLite Store resource. The returned Resource must
// either be passed to an Agent as owned configuration or released explicitly.
// ctx bounds construction only and is not retained.
func Open(ctx context.Context, config Config) (framework.Resource[agent.Store], error) {
	return agent.OpenSQLiteStore(ctx, config.Path)
}
