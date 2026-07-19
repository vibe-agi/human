package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/humantest"
	"github.com/vibe-agi/human/workspace"
	workspacesqlite "github.com/vibe-agi/human/workspace/sqlite"
)

func TestStoreConformance(t *testing.T) {
	humantest.TestWorkspaceStore(t, func(
		ctx context.Context,
		test testing.TB,
	) (workspace.Store, framework.ReleaseFunc, error) {
		resource, err := workspacesqlite.Open(ctx, workspacesqlite.Config{
			Path: filepath.Join(test.TempDir(), "workspace.db"),
		})
		if err != nil {
			return nil, nil, err
		}
		store, err := resource.Value()
		if err != nil {
			_ = resource.Release(context.Background())
			return nil, nil, err
		}
		return store, resource.Release, nil
	})
}
