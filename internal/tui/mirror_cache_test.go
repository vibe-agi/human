package tui

import (
	"fmt"
	"testing"
)

func TestFilesystemMirrorManagerKeepsOneWorkspaceInstancePerNamespace(t *testing.T) {
	manager := newFilesystemMirrorManager(t.TempDir())
	first, err := manager.Open("caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if _, err := manager.Open("other-caller", fmt.Sprintf("workspace-%d", index)); err != nil {
			t.Fatalf("open other mirror %d: %v", index, err)
		}
	}
	second, err := manager.Open("caller", "workspace")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("manager returned two independently locked workspaces for one namespace")
	}
}

func TestModelPrunesMirrorsOutsideLiveSessions(t *testing.T) {
	live := testAssignment()
	live.WorkspaceKey = "live-workspace"
	model := New(newFakeClient())
	model.active = &live
	liveNamespace := mirrorNamespace(live.CallerID, live.WorkspaceKey)
	staleNamespace := mirrorNamespace("stale-caller", "stale-workspace")
	model.mirrors[liveNamespace] = nil
	model.mirrors[staleNamespace] = nil

	model.pruneMirrorCache()
	if _, ok := model.mirrors[liveNamespace]; !ok {
		t.Fatal("mirror pruning removed the active workspace")
	}
	if _, ok := model.mirrors[staleNamespace]; ok {
		t.Fatal("mirror pruning retained an inactive workspace")
	}
}
