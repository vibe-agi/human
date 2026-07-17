package userdata

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlatformRootUsesUserDataConventions(t *testing.T) {
	home := filepath.Join(string(filepath.Separator), "home", "expert")
	tests := []struct {
		name string
		goos string
		env  map[string]string
		want string
	}{
		{name: "macOS", goos: "darwin", want: filepath.Join(home, "Library", "Application Support", "Human")},
		{name: "Linux", goos: "linux", want: filepath.Join(home, ".local", "share", "human")},
		{name: "XDG", goos: "linux", env: map[string]string{"XDG_DATA_HOME": filepath.Join(home, "data")}, want: filepath.Join(home, "data", "human")},
		{name: "Windows", goos: "windows", env: map[string]string{"LOCALAPPDATA": filepath.Join(home, "Local")}, want: filepath.Join(home, "Local", "Human")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := platformRoot(test.goos, home, func(key string) string { return test.env[key] })
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("platform root = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRootAcceptsOnlyAbsoluteOverride(t *testing.T) {
	t.Setenv(dataHomeEnvironment, "relative/state")
	if _, err := Root(); err == nil {
		t.Fatal("Root accepted a relative HUMAN_DATA_HOME")
	}
	want := filepath.Join(t.TempDir(), "product-data")
	t.Setenv(dataHomeEnvironment, want)
	got, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Root = %q, want %q", got, want)
	}
}

func TestResolveGitWorkspaceUsesRealNearestRoot(t *testing.T) {
	root := t.TempDir()
	project := filepath.Join(root, "project")
	nested := filepath.Join(project, "a", "b")
	if err := os.MkdirAll(filepath.Join(project, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(nested, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	got, err := ResolveGitWorkspace(alias)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Git workspace = %q, want %q", got, want)
	}
}

func TestResolveGitWorkspaceKeepsNonGitDirectory(t *testing.T) {
	directory := t.TempDir()
	got, err := ResolveGitWorkspace(directory)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("non-Git workspace = %q, want %q", got, want)
	}
}

func TestResolveGitWorkspaceRejectsSymlinkMarker(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "git-metadata")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(root, "project")
	if err := os.Mkdir(project, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(project, ".git")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ResolveGitWorkspace(project); err == nil || !strings.Contains(err.Error(), "regular file or directory") {
		t.Fatalf("symlink Git marker error = %v", err)
	}
}

func TestResolveGitWorkspaceAcceptsRegularFileMarker(t *testing.T) {
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, ".git"), []byte("gitdir: elsewhere\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveGitWorkspace(project)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(project)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("worktree root = %q, want %q", got, want)
	}
}

func TestWorkspacePathIsStablePrivateScope(t *testing.T) {
	data := t.TempDir()
	t.Setenv(dataHomeEnvironment, data)
	workspace := filepath.Join(t.TempDir(), "customer")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := WorkspacePath("shim", workspace, "caller-ledger.db")
	if err != nil {
		t.Fatal(err)
	}
	second, err := WorkspacePath("shim", filepath.Clean(workspace), "caller-ledger.db")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("stable workspace paths differ: %q != %q", first, second)
	}
	identity, err := canonicalWorkspaceIdentity(workspace)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(identity))
	want := filepath.Join(data, "shim", "workspaces", hex.EncodeToString(sum[:]), "caller-ledger.db")
	if first != want {
		t.Fatalf("workspace path = %q, want %q", first, want)
	}
	if _, err := WorkspacePath("shim", "relative", "ledger.db"); err == nil {
		t.Fatal("WorkspacePath accepted relative identity")
	}
}

func TestWorkspaceKeyIsStableAndDoesNotRevealPath(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "customers", "secret-project")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := WorkspaceKey(workspace)
	if err != nil {
		t.Fatal(err)
	}
	second, err := WorkspaceKey(filepath.Clean(workspace))
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("stable workspace keys differ: %q != %q", first, second)
	}
	if strings.Contains(first, "customers") || strings.Contains(first, "secret-project") {
		t.Fatalf("workspace key reveals its source path: %q", first)
	}
	if len(first) != len("workspace-")+sha256.Size*2 {
		t.Fatalf("workspace key length = %d", len(first))
	}
	if _, err := WorkspaceKey("relative"); err == nil {
		t.Fatal("WorkspaceKey accepted relative identity")
	}
}

func TestWorkspaceDigestNormalizesWindowsPathCase(t *testing.T) {
	upper := filepath.Join(string(filepath.Separator), "Users", "Expert", "Project")
	lower := strings.ToLower(upper)
	first, err := workspaceDigestForOS("windows", upper)
	if err != nil {
		t.Fatal(err)
	}
	second, err := workspaceDigestForOS("windows", lower)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("Windows casing aliases have different digests: %q != %q", first, second)
	}
	unixFirst, err := workspaceDigestForOS("linux", upper)
	if err != nil {
		t.Fatal(err)
	}
	unixSecond, err := workspaceDigestForOS("linux", lower)
	if err != nil {
		t.Fatal(err)
	}
	if unixFirst == unixSecond {
		t.Fatal("case-sensitive platform paths unexpectedly share a digest")
	}
}

func TestWorkspaceDigestNormalizesUnicodeNFC(t *testing.T) {
	composed := filepath.Join(string(filepath.Separator), "customers", "café")
	decomposed := filepath.Join(string(filepath.Separator), "customers", "cafe\u0301")
	first, err := workspaceDigestForOS("linux", composed)
	if err != nil {
		t.Fatal(err)
	}
	second, err := workspaceDigestForOS("linux", decomposed)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("Unicode normalization aliases have different digests: %q != %q", first, second)
	}
}

func TestNormalizeWorkspaceComponentFoldsOnlyWhenRequested(t *testing.T) {
	composedUpper := "CAFÉ"
	decomposedLower := "cafe\u0301"
	if got := normalizeWorkspaceComponent(decomposedLower, false); got != "café" {
		t.Fatalf("NFC component = %q, want %q", got, "café")
	}
	if got := normalizeWorkspaceComponent(composedUpper, false); got != composedUpper {
		t.Fatalf("case-sensitive component = %q, want %q", got, composedUpper)
	}
	if got := normalizeWorkspaceComponent(composedUpper, true); got != "café" {
		t.Fatalf("case-folded component = %q, want %q", got, "café")
	}
}

func TestPathRejectsEscape(t *testing.T) {
	t.Setenv(dataHomeEnvironment, t.TempDir())
	for _, element := range []string{"", ".", "..", filepath.Join("..", "escape"), filepath.Join(string(filepath.Separator), "absolute")} {
		if _, err := Path(element); err == nil {
			t.Fatalf("Path accepted %q", element)
		}
	}
}
