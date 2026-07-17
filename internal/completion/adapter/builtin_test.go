package adapter

import "testing"

func TestDefaultRegistryContainsExactBuiltinProfiles(t *testing.T) {
	t.Parallel()
	registry := NewDefaultRegistry()
	profile, ok := registry.Resolve(HumanShimID, HumanShimVersion)
	if !ok {
		t.Fatal("human shim adapter is missing")
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"human_read_file", "human_search", "human_write_file", "human_edit_file",
		"human_delete_file", "human_rename_file", "human_exec",
	} {
		if !profile.AllowsTool(name) {
			t.Fatalf("profile does not allow %q", name)
		}
	}
	if _, ok := registry.Resolve(HumanShimID, "2"); ok {
		t.Fatal("unknown shim version inherited capabilities")
	}

	opencode, ok := registry.Resolve(OpenCodeID, OpenCodeVersion)
	if !ok {
		t.Fatal("OpenCode 1.17.18 adapter is missing")
	}
	if err := opencode.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"read", "write", "edit", "bash"} {
		if !opencode.AllowsTool(name) {
			t.Fatalf("OpenCode profile does not allow %q", name)
		}
	}
	if _, ok := registry.Resolve(OpenCodeID, "1.17.19"); ok {
		t.Fatal("unknown OpenCode version inherited capabilities")
	}
}
