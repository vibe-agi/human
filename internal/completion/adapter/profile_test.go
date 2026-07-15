package adapter

import "testing"

func TestRegistryRequiresExactProfile(t *testing.T) {
	t.Parallel()
	registry := NewRegistry()
	profile := Profile{
		HarnessID:      "harness",
		HarnessVersion: "1.0",
		Read:           &Tool{Name: "read_file"},
		Edit: &EditTool{
			Tool: Tool{Name: "edit_file"}, Semantics: EditExact,
			OldField: "old_string", NewField: "new_string",
		},
		ErrorShape: "tool_result.is_error",
	}
	if err := registry.Register(profile); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Resolve("harness", "1.0"); !ok {
		t.Fatal("registered profile was not resolved")
	}
	if _, ok := registry.Resolve("harness", "1.1"); ok {
		t.Fatal("unknown version inherited capabilities")
	}
	if err := registry.Register(profile); err == nil {
		t.Fatal("duplicate profile accepted")
	}
}

func TestProfileRejectsAmbiguousTools(t *testing.T) {
	t.Parallel()
	profile := Profile{
		HarnessID: "h", HarnessVersion: "v",
		Read:       &Tool{Name: "file"},
		Write:      &Tool{Name: "file"},
		ErrorShape: "error",
	}
	if err := profile.Validate(); err == nil {
		t.Fatal("duplicate tool mapping accepted")
	}
}

func TestProfileRejectsNoCapabilities(t *testing.T) {
	t.Parallel()
	profile := Profile{HarnessID: "h", HarnessVersion: "v", ErrorShape: "error"}
	if err := profile.Validate(); err == nil {
		t.Fatal("empty capability profile accepted")
	}
}
