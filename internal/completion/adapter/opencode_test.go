package adapter

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestOpenCode11718ProfileMatchesCapturedToolSchemas(t *testing.T) {
	t.Parallel()

	profile := OpenCode11718Profile()
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	if profile.Key() != "opencode@1.17.18" {
		t.Fatalf("profile key = %q", profile.Key())
	}
	if profile.PathStyle != PathAbsolute || profile.ResultCodec != ResultOpenCode11718 {
		t.Fatalf("workspace contract = path %q, result %q", profile.PathStyle, profile.ResultCodec)
	}
	if profile.Search != nil || profile.Delete != nil || profile.Rename != nil {
		t.Fatal("uncaptured OpenCode capabilities were declared")
	}

	want := map[string]map[string]string{
		"read":  {"path": "filePath"},
		"write": {"path": "filePath", "content": "content"},
		"edit":  {"path": "filePath", "old": "oldString", "new": "newString"},
		"bash":  {"command": "command", "cwd": "workdir", "timeout_ms": "timeout"},
	}
	got := map[string]map[string]string{
		profile.Read.Name:  profile.Read.Args,
		profile.Write.Name: profile.Write.Args,
		profile.Edit.Name:  profile.Edit.Args,
		profile.Exec.Name:  profile.Exec.Args,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tool mapping = %#v, want %#v", got, want)
	}
	if profile.Edit.Semantics != EditExact || profile.Edit.OldField != "oldString" || profile.Edit.NewField != "newString" {
		t.Fatalf("edit contract = %+v", profile.Edit)
	}
	if profile.Exec.CWDField != "workdir" || profile.Exec.TimeoutField != "timeout" {
		t.Fatalf("bash contract = %+v", profile.Exec)
	}

	fieldTypes := map[string]map[string]string{
		"read":  {"filePath": "string"},
		"write": {"filePath": "string", "content": "string"},
		"edit":  {"filePath": "string", "oldString": "string", "newString": "string"},
		"bash":  {"command": "string", "workdir": "string", "timeout": "integer"},
	}
	for toolName, mapping := range want {
		properties := capturedOpenCode11718Properties(t, toolName)
		for semantic, field := range mapping {
			property, ok := properties[field]
			if !ok {
				t.Errorf("%s mapping %q -> %q is absent from captured schema", toolName, semantic, field)
				continue
			}
			if property.Type != fieldTypes[toolName][field] {
				t.Errorf("%s field %q type = %q, want %q", toolName, field, property.Type, fieldTypes[toolName][field])
			}
		}
	}
}

func TestWorkspaceResultCodecIsReservedForExactHarnessVersion(t *testing.T) {
	t.Parallel()
	profile := OpenCode11718Profile()
	profile.HarnessVersion = "1.17.19"
	if err := profile.Validate(); err == nil {
		t.Fatal("OpenCode 1.17.18 result codec was reused by another version")
	}
	profile = OpenCode11718Profile()
	profile.PathStyle = PathWorkspaceVirtual
	if err := profile.Validate(); err == nil {
		t.Fatal("OpenCode result codec accepted inferred virtual-path semantics")
	}
}

func TestOpenCode11718ProfileDoesNotInferUncapturedTools(t *testing.T) {
	t.Parallel()
	profile := OpenCode11718Profile()
	for _, name := range []string{"search", "grep", "glob", "delete", "rename"} {
		if profile.AllowsTool(name) {
			t.Fatalf("uncaptured tool %q was allowed", name)
		}
	}
}

func TestOpenCode11718ClassifiesNativeToolAuthority(t *testing.T) {
	t.Parallel()
	profile := OpenCode11718Profile()
	tests := []struct {
		name string
		want ToolAuthorization
	}{
		{name: "edit", want: ToolAuthorizationStandard},
		{name: "todowrite", want: ToolAuthorizationStandard},
		{name: "bash", want: ToolAuthorizationPrivileged},
		{name: "task", want: ToolAuthorizationPrivileged},
		{name: "webfetch", want: ToolAuthorizationPrivileged},
	}
	for _, test := range tests {
		got, ok := profile.AuthorizeTool(test.name)
		if !ok || got != test.want {
			t.Errorf("authorization for %q = %q, %v; want %q, true", test.name, got, ok, test.want)
		}
	}
	if got, ok := profile.AuthorizeTool("custom_mcp_tool"); ok {
		t.Fatalf("unreviewed custom tool classified as %q", got)
	}
}

type capturedProperty struct {
	Type string `json:"type"`
}

func capturedOpenCode11718Properties(t *testing.T, toolName string) map[string]capturedProperty {
	t.Helper()
	// Normalized from the OpenCode 1.17.18 schemas captured by the real Chat
	// harness run. Descriptions and numeric bounds are intentionally omitted;
	// capability mapping depends only on field names and JSON types.
	schemas := map[string]string{
		"read":  `{"type":"object","properties":{"filePath":{"type":"string"},"offset":{"type":"integer"},"limit":{"type":"integer"}},"required":["filePath"]}`,
		"write": `{"type":"object","properties":{"content":{"type":"string"},"filePath":{"type":"string"}},"required":["content","filePath"]}`,
		"edit":  `{"type":"object","properties":{"filePath":{"type":"string"},"oldString":{"type":"string"},"newString":{"type":"string"},"replaceAll":{"type":"boolean"}},"required":["filePath","oldString","newString"]}`,
		"bash":  `{"type":"object","properties":{"command":{"type":"string"},"timeout":{"type":"integer"},"workdir":{"type":"string"}},"required":["command"]}`,
	}
	var schema struct {
		Properties map[string]capturedProperty `json:"properties"`
	}
	if err := json.Unmarshal([]byte(schemas[toolName]), &schema); err != nil {
		t.Fatalf("decode captured %s schema: %v", toolName, err)
	}
	return schema.Properties
}
