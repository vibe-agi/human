package humancmd

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveSecretFromEnvironment(t *testing.T) {
	withoutEnvironment(t, "HUMAN_TEST_SECRET")
	t.Setenv("HUMAN_TEST_SECRET", "  secret-value\n")

	got, err := resolveSecret("HUMAN_TEST_SECRET", "", "test token")
	if err != nil {
		t.Fatal(err)
	}
	if got != "secret-value" {
		t.Fatalf("secret = %q", got)
	}
}

func TestResolveSecretFromPrivateFile(t *testing.T) {
	withoutEnvironment(t, "HUMAN_TEST_SECRET")
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("secret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := resolveSecret("HUMAN_TEST_SECRET", path, "test token")
	if err != nil {
		t.Fatal(err)
	}
	if got != "secret-value" {
		t.Fatalf("secret = %q", got)
	}
}

func TestResolveSecretRejectsAmbiguousOrUnsafeSources(t *testing.T) {
	withoutEnvironment(t, "HUMAN_TEST_SECRET")
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("secret-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUMAN_TEST_SECRET", "environment-value")
	if _, err := resolveSecret("HUMAN_TEST_SECRET", path, "test token"); err == nil || !strings.Contains(err.Error(), "configured twice") {
		t.Fatalf("ambiguous source error = %v", err)
	}

	if runtime.GOOS == "windows" {
		return
	}
	withoutEnvironment(t, "HUMAN_TEST_SECRET")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveSecret("HUMAN_TEST_SECRET", path, "test token"); err == nil || !strings.Contains(err.Error(), "group or other") {
		t.Fatalf("unsafe file error = %v", err)
	}
}

func TestResolveSecretRejectsWhitespaceInsideToken(t *testing.T) {
	withoutEnvironment(t, "HUMAN_TEST_SECRET")
	t.Setenv("HUMAN_TEST_SECRET", "two values")
	if _, err := resolveSecret("HUMAN_TEST_SECRET", "", "test token"); err == nil || !strings.Contains(err.Error(), "whitespace") {
		t.Fatalf("whitespace error = %v", err)
	}
}

func withoutEnvironment(t *testing.T, name string) {
	t.Helper()
	value, exists := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if exists {
			_ = os.Setenv(name, value)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}
