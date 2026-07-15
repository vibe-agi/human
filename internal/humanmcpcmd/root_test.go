package humanmcpcmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	command := New()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetArgs([]string{"version"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != Version {
		t.Fatalf("version output = %q", output.String())
	}
}

func TestResolvePathRejectsUnsupportedHomeSyntax(t *testing.T) {
	if _, err := resolvePath("~somebody/repo", "repository", false); err == nil {
		t.Fatal("unsupported home syntax was accepted")
	}
	if value, err := resolvePath("", "state", true); err != nil || value != "" {
		t.Fatalf("optional path = %q, %v", value, err)
	}
}

func TestInsecureA2AHTTPRequiresExplicitFlag(t *testing.T) {
	command := New()
	flag := command.Flags().Lookup("allow-insecure-a2a-http")
	if flag == nil {
		t.Fatal("allow-insecure-a2a-http flag is not registered")
	}
	if flag.DefValue != "false" {
		t.Fatalf("allow-insecure-a2a-http default = %q; want false", flag.DefValue)
	}
}
