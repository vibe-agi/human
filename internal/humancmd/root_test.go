package humancmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCommandExposesCompletionFlowOnly(t *testing.T) {
	t.Parallel()
	subcommands := New().Commands()
	if len(subcommands) != 1 || subcommands[0].Name() != "shim" {
		t.Fatalf("subcommands = %v; want only shim", subcommands)
	}
}

func TestWorkerMirrorRootDefaultsToDocumentedLocation(t *testing.T) {
	t.Parallel()
	command := New()
	flag := command.Flags().Lookup("mirror-root")
	if flag == nil || flag.DefValue != "~/mirror" {
		t.Fatalf("mirror-root flag = %+v", flag)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveMirrorRoot(flag.DefValue)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != filepath.Join(home, "mirror") {
		t.Fatalf("resolved mirror root = %q, want %q", resolved, filepath.Join(home, "mirror"))
	}
}

func TestWorkerOutboxDefaultsToPrivateStateDirectory(t *testing.T) {
	t.Parallel()
	command := New()
	flag := command.Flags().Lookup("outbox")
	if flag == nil || flag.DefValue != "~/.human/worker-outbox.db" {
		t.Fatalf("outbox flag = %+v", flag)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolvePrivatePath(flag.DefValue, "worker outbox")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != filepath.Join(home, ".human", "worker-outbox.db") {
		t.Fatalf("resolved outbox = %q", resolved)
	}
}

func TestResolveMirrorRootRejectsAmbiguousUserExpansion(t *testing.T) {
	t.Parallel()
	if _, err := resolveMirrorRoot("~someone/mirror"); err == nil {
		t.Fatal("expected ~user mirror root to be rejected")
	}
}
