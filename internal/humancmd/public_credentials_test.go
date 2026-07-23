package humancmd

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestEnsurePublicCallerTokenPersistsAndReloads(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "private", "caller-token.json")

	first, err := ensurePublicCallerToken(path, false)
	if err != nil || first == "" {
		t.Fatalf("ensure = %q, err=%v", first, err)
	}
	// A restart re-reads the same token so the OpenCode provider config keeps working.
	second, err := ensurePublicCallerToken(path, false)
	if err != nil || second != first {
		t.Fatalf("reload = %q (err %v), want stable %q", second, err, first)
	}
	// A reset mints a fresh token.
	reset, err := ensurePublicCallerToken(path, true)
	if err != nil || reset == first {
		t.Fatalf("reset = %q (err %v), want a new token", reset, err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("token file mode = %v, want 0600", info.Mode().Perm())
		}
	}
}

func TestReadPublicCredentialsRejectsBroadPermissionsAndUnknownFields(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits")
	}
	path := filepath.Join(t.TempDir(), "caller-token.json")
	valid := `{"version":1,"caller_token":"abc123def456"}`
	if err := os.WriteFile(path, []byte(valid), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readPublicCredentials(path); err == nil {
		t.Fatal("read accepted a group/other-readable token file")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	token, found, err := readPublicCredentials(path)
	if err != nil || !found || token != "abc123def456" {
		t.Fatalf("read = %q found=%v err=%v", token, found, err)
	}
	withUnknown := `{"version":1,"caller_token":"abc123def456","extra":true}`
	if err := os.WriteFile(path, []byte(withUnknown), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readPublicCredentials(path); err == nil {
		t.Fatal("read accepted unknown fields")
	}
}
