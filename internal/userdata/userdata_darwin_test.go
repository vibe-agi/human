//go:build darwin

package userdata

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceDigestHonorsDarwinVolumeCaseBehavior(t *testing.T) {
	parent := t.TempDir()
	exact := filepath.Join(parent, "WorkspaceCaseProbe")
	alias := filepath.Join(parent, "workspacecaseprobe")
	if err := os.Mkdir(exact, 0o700); err != nil {
		t.Fatal(err)
	}

	exactInfo, err := os.Lstat(exact)
	if err != nil {
		t.Fatal(err)
	}
	aliasInfo, aliasErr := os.Lstat(alias)
	caseInsensitive := aliasErr == nil && os.SameFile(exactInfo, aliasInfo)
	if aliasErr != nil && !errors.Is(aliasErr, os.ErrNotExist) {
		t.Fatal(aliasErr)
	}
	if !caseInsensitive {
		if err := os.Mkdir(alias, 0o700); err != nil {
			t.Fatalf("create case-distinct workspace on case-sensitive volume: %v", err)
		}
	}

	exactDigest, err := workspaceDigest(exact)
	if err != nil {
		t.Fatal(err)
	}
	aliasDigest, err := workspaceDigest(alias)
	if err != nil {
		t.Fatal(err)
	}
	if caseInsensitive && exactDigest != aliasDigest {
		t.Fatalf("case-insensitive Darwin aliases have different digests: %q != %q", exactDigest, aliasDigest)
	}
	if !caseInsensitive && exactDigest == aliasDigest {
		t.Fatalf("case-sensitive Darwin workspaces share a digest: %q", exactDigest)
	}
}
