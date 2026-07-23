package harnessresolver_test

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/callerhttp"
	"github.com/vibe-agi/human/llm/callerhttp/harnessresolver"
)

const (
	workspaceRoot = "/tmp/ws"
	workspaceKey  = "ws-local"
	opencodeUA    = "opencode/1.17.18 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"
	toolBody      = `{"model":"human","stream":true,"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"fix the bug"}]}`
	codexUA       = "codex_exec/0.145.0"
	codexTurnID   = "123e4567-e89b-12d3-a456-426614174000"
	codexToolBody = `{"model":"human","input":"fix it","tools":[{"type":"function","name":"exec_command","parameters":{"type":"object"}}]}`
	claudeUA      = "claude-cli/2.1.217 (external, sdk-cli)"
	claudeSession = "223e4567-e89b-12d3-a456-426614174000"
)

var stablePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func newResolver(t *testing.T) *harnessresolver.Resolver {
	t.Helper()
	resolver, err := harnessresolver.New(harnessresolver.Config{
		WorkspaceKey: workspaceKey, ExecAllowed: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return resolver
}

func codexRequest(caller, body, metadata string) callerhttp.ResolutionRequest {
	header := http.Header{}
	header.Set("User-Agent", codexUA)
	if metadata != "" {
		header.Set("X-Codex-Turn-Metadata", metadata)
	}
	return callerhttp.ResolutionRequest{
		CallerID: llm.CallerID(caller),
		Route: callerhttp.Route{
			Method: "POST", Path: "/v1/responses", CodecID: "openai.responses",
		},
		Header: header,
		Body:   []byte(body),
	}
}

func request(caller string, header http.Header, body string) callerhttp.ResolutionRequest {
	return callerhttp.ResolutionRequest{
		CallerID: llm.CallerID(caller),
		Route:    callerhttp.Route{Method: "POST", Path: "/v1/chat/completions", CodecID: "openai.chat"},
		Header:   header,
		Body:     []byte(body),
	}
}

func claudeRequest(caller, session, message string) callerhttp.ResolutionRequest {
	header := http.Header{}
	header.Set("User-Agent", claudeUA)
	header.Set("X-Claude-Code-Session-Id", session)
	body := `{"model":"human","metadata":{"user_id":"{\"device_id\":\"device-a\",\"account_uuid\":\"\",\"session_id\":\"` + session + `\"}"},` +
		`"tools":[{"name":"Bash","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"` + message + `"}]}`
	return callerhttp.ResolutionRequest{
		CallerID: llm.CallerID(caller),
		Route:    callerhttp.Route{Method: "POST", Path: "/v1/messages", CodecID: "anthropic.messages"},
		Header:   header,
		Body:     []byte(body),
	}
}

func claudeWorkspaceRequest(caller, session, message string) callerhttp.ResolutionRequest {
	request := claudeRequest(caller, session, message)
	request.Body = []byte(`{"model":"human","metadata":{"user_id":"{\"device_id\":\"device-a\",\"account_uuid\":\"\",\"session_id\":\"` + session + `\"}"},` +
		`"tools":[` +
		`{"name":"Write","input_schema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"file_path":{"type":"string"},"content":{"type":"string"}},"required":["file_path","content"],"additionalProperties":false}},` +
		`{"name":"Edit","input_schema":{"type":"object","properties":{"file_path":{"type":"string"},"old_string":{"type":"string"},"new_string":{"type":"string"},"replace_all":{"type":"boolean"}},"required":["file_path","old_string","new_string"],"additionalProperties":false}}` +
		`],"messages":[{"role":"user","content":"` + message + `"}]}`)
	return request
}

func opencodeHeader(session string) http.Header {
	header := http.Header{}
	header.Set("User-Agent", opencodeUA)
	header.Set("X-Session-Id", session)
	return header
}

func resolve(t *testing.T, resolver *harnessresolver.Resolver, req callerhttp.ResolutionRequest) callerhttp.Resolution {
	t.Helper()
	resolution, err := resolver.ResolveRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveRequest: %v", err)
	}
	return resolution
}

func TestOpenCodeWorkspaceAffinityOmitsTaskID(t *testing.T) {
	resolver := newResolver(t)
	resolution := resolve(t, resolver, request("caller-a", opencodeHeader("ses_abc123"), toolBody))

	task := resolution.Task
	if task.CapabilityTier != llm.TierWorkspace {
		t.Fatalf("tier = %q, want workspace", task.CapabilityTier)
	}
	if task.HarnessID != "opencode" || task.HarnessVersion != "1.17.18" {
		t.Fatalf("harness = %q/%q, want opencode/1.17.18", task.HarnessID, task.HarnessVersion)
	}
	if task.HarnessSessionID != "ses_abc123" {
		t.Fatalf("session = %q, want ses_abc123", task.HarnessSessionID)
	}
	if task.WorkspaceKey != workspaceKey || !task.ExecAllowed {
		t.Fatalf("workspace binding = %+v", task)
	}
	// TaskID MUST be empty: the core's FindOpenTask affinity resolution owns task
	// continuation. A resolver-supplied TaskID would bypass it.
	if task.TaskID != "" {
		t.Fatalf("TaskID = %q, want empty (core resolves affinity)", task.TaskID)
	}
	if !stablePattern.MatchString(string(resolution.IdempotencyKey)) {
		t.Fatalf("derived key %q is not a stable key", resolution.IdempotencyKey)
	}
}

func TestCodexResponsesTurnGetsStableRemoteToolsAffinity(t *testing.T) {
	resolver := newResolver(t)
	metadata := `{"request_kind":"turn","turn_id":"` + codexTurnID + `"}`
	first := resolve(t, resolver, codexRequest("caller-a", codexToolBody, metadata))
	second := resolve(t, resolver, codexRequest(
		"caller-a",
		`{"tools":[{"parameters":{"type":"object"},"name":"exec_command","type":"function"}],"input":"fix it","model":"human"}`,
		metadata,
	))
	if first.Task.CapabilityTier != llm.TierRemoteTools || first.Task.HarnessID != "codex" ||
		first.Task.HarnessVersion != "0.145.0" || first.Task.HarnessSessionID != codexTurnID ||
		first.Task.WorkspaceKey != workspaceKey || !first.Task.ExecAllowed {
		t.Fatalf("Codex task affinity = %+v", first.Task)
	}
	if first.IdempotencyKey != second.IdempotencyKey {
		t.Fatalf("Codex semantic retry keys differ: %q != %q", first.IdempotencyKey, second.IdempotencyKey)
	}
	if !stablePattern.MatchString(string(first.IdempotencyKey)) {
		t.Fatalf("Codex key %q is not stable", first.IdempotencyKey)
	}
}

func TestCodexExactApplyPatchDeclarationGrantsWorkspace(t *testing.T) {
	resolver := newResolver(t)
	metadata := `{"request_kind":"turn","turn_id":"` + codexTurnID + `"}`
	body := `{"model":"human","input":"edit it","tools":[{"type":"custom","name":"apply_patch",` +
		`"format":{"type":"grammar","syntax":"lark","definition":"start: \"*** Begin Patch\" update add delete \"*** End Patch\"\\nupdate: \"*** Update File: \"\\nadd: \"*** Add File: \"\\ndelete: \"*** Delete File: \""}}]}`
	resolution := resolve(t, resolver, codexRequest("caller-a", body, metadata))
	if resolution.Task.CapabilityTier != llm.TierWorkspace ||
		resolution.Task.HarnessID != "codex" || resolution.Task.HarnessVersion != "0.145.0" ||
		resolution.Task.HarnessSessionID != codexTurnID {
		t.Fatalf("Codex workspace affinity = %+v", resolution.Task)
	}
}

func TestCodexApplyPatchSchemaDriftFailsClosedToRemoteTools(t *testing.T) {
	resolver := newResolver(t)
	metadata := `{"request_kind":"turn","turn_id":"` + codexTurnID + `"}`
	body := `{"model":"human","input":"edit it","tools":[{"type":"custom","name":"apply_patch",` +
		`"format":{"type":"text"}}]}`
	resolution := resolve(t, resolver, codexRequest("caller-a", body, metadata))
	if resolution.Task.CapabilityTier != llm.TierRemoteTools {
		t.Fatalf("Codex schema-drift tier = %q, want remote_tools", resolution.Task.CapabilityTier)
	}
}

func TestCodexWithoutDeclaredToolsIsChat(t *testing.T) {
	resolver := newResolver(t)
	metadata := `{"request_kind":"turn","turn_id":"` + codexTurnID + `"}`
	resolution := resolve(t, resolver, codexRequest(
		"caller-a", `{"model":"human","input":"summarize"}`, metadata,
	))
	if resolution.Task.CapabilityTier != llm.TierChat || resolution.Task.HarnessSessionID != "" {
		t.Fatalf("tool-free Codex request gained affinity: %+v", resolution.Task)
	}
}

func TestClaudeMessagesSessionGetsStableRemoteToolsAffinity(t *testing.T) {
	resolver := newResolver(t)
	firstRequest := claudeRequest("caller-a", claudeSession, "fix it")
	secondRequest := claudeRequest("caller-a", claudeSession, "fix it")
	// Reorder the top-level object to prove retry identity uses JSON semantics,
	// not the caller's incidental serialization order.
	secondRequest.Body = []byte(`{"messages":[{"content":"fix it","role":"user"}],"tools":[{"input_schema":{"type":"object"},"name":"Bash"}],` +
		`"metadata":{"user_id":"{\"device_id\":\"device-a\",\"account_uuid\":\"\",\"session_id\":\"` + claudeSession + `\"}"},"model":"human"}`)
	first := resolve(t, resolver, firstRequest)
	second := resolve(t, resolver, secondRequest)
	if first.Task.CapabilityTier != llm.TierRemoteTools || first.Task.HarnessID != "claude-code" ||
		first.Task.HarnessVersion != "2.1.217" || first.Task.HarnessSessionID != claudeSession ||
		first.Task.WorkspaceKey != workspaceKey || !first.Task.ExecAllowed {
		t.Fatalf("Claude task affinity = %+v", first.Task)
	}
	if first.IdempotencyKey != second.IdempotencyKey {
		t.Fatalf("Claude semantic retry keys differ: %q != %q", first.IdempotencyKey, second.IdempotencyKey)
	}
}

func TestClaudeExactNativeFileSchemasGrantWorkspace(t *testing.T) {
	resolver := newResolver(t)
	resolution := resolve(t, resolver, claudeWorkspaceRequest("caller-a", claudeSession, "edit it"))
	if resolution.Task.CapabilityTier != llm.TierWorkspace ||
		resolution.Task.HarnessID != "claude-code" || resolution.Task.HarnessVersion != "2.1.217" ||
		resolution.Task.HarnessSessionID != claudeSession {
		t.Fatalf("Claude workspace affinity = %+v", resolution.Task)
	}
}

func TestClaudeNativeFileSchemaDriftFailsClosedToRemoteTools(t *testing.T) {
	resolver := newResolver(t)
	request := claudeWorkspaceRequest("caller-a", claudeSession, "edit it")
	// A newly-added input field changes what a caller can ask the native tool to
	// do. Keep generic tools available, but do not project mirror bytes through
	// an unreviewed schema/version combination.
	request.Body = []byte(strings.Replace(string(request.Body),
		`"content":{"type":"string"}},"required"`,
		`"content":{"type":"string"},"encoding":{"type":"string"}},"required"`, 1))
	resolution := resolve(t, resolver, request)
	if resolution.Task.CapabilityTier != llm.TierRemoteTools {
		t.Fatalf("schema-drift tier = %q, want remote_tools", resolution.Task.CapabilityTier)
	}
}

func TestClaudeSessionCopiesMustMatch(t *testing.T) {
	resolver := newResolver(t)
	request := claudeRequest("caller-a", claudeSession, "fix it")
	request.Header.Set("X-Claude-Code-Session-Id", codexTurnID)
	if _, err := resolver.ResolveRequest(context.Background(), request); err == nil {
		t.Fatal("conflicting Claude session identities must fail closed")
	}
}

func TestClaudeMalformedIdentityFailsClosed(t *testing.T) {
	resolver := newResolver(t)
	tests := []struct {
		name   string
		mutate func(*callerhttp.ResolutionRequest)
	}{
		{
			name: "missing session header",
			mutate: func(request *callerhttp.ResolutionRequest) {
				request.Header.Del("X-Claude-Code-Session-Id")
			},
		},
		{
			name: "duplicate session header",
			mutate: func(request *callerhttp.ResolutionRequest) {
				request.Header.Add("X-Claude-Code-Session-Id", claudeSession)
			},
		},
		{
			name: "ambiguous embedded identity",
			mutate: func(request *callerhttp.ResolutionRequest) {
				request.Body = []byte(`{"model":"human","metadata":{"user_id":"{\"session_id\":\"` + claudeSession + `\",\"session_id\":\"` + claudeSession + `\"}"},"tools":[{}]}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := claudeRequest("caller-a", claudeSession, "fix it")
			test.mutate(&request)
			if _, err := resolver.ResolveRequest(context.Background(), request); err == nil {
				t.Fatal("malformed Claude identity must fail closed")
			}
		})
	}
}

func TestClaudeWithoutDeclaredToolsIsChat(t *testing.T) {
	resolver := newResolver(t)
	request := claudeRequest("caller-a", claudeSession, "summarize")
	request.Body = []byte(`{"model":"human","metadata":{"user_id":"{\"session_id\":\"` + claudeSession + `\"}"},"messages":[{"role":"user","content":"summarize"}]}`)
	resolution := resolve(t, resolver, request)
	if resolution.Task.CapabilityTier != llm.TierChat || resolution.Task.HarnessSessionID != "" {
		t.Fatalf("tool-free Claude request gained affinity: %+v", resolution.Task)
	}
}

func TestUnknownClaudeVersionKeepsRetryDedupButNoTaskAffinity(t *testing.T) {
	resolver := newResolver(t)
	request := claudeRequest("caller-a", claudeSession, "fix it")
	request.Header.Set("User-Agent", "claude-cli/9.9.9 (external, sdk-cli)")
	resolution := resolve(t, resolver, request)
	if resolution.Task.CapabilityTier != llm.TierChat || resolution.Task.HarnessSessionID != "" {
		t.Fatalf("unknown Claude version gained task affinity: %+v", resolution.Task)
	}
	if !stablePattern.MatchString(string(resolution.IdempotencyKey)) {
		t.Fatalf("unknown Claude version lost stable retry key: %q", resolution.IdempotencyKey)
	}
}

func TestUnknownCodexVersionKeepsRetryDedupButNoTaskAffinity(t *testing.T) {
	resolver := newResolver(t)
	request := codexRequest("caller-a", `{"model":"human","input":"fix it"}`,
		`{"request_kind":"turn","turn_id":"`+codexTurnID+`"}`)
	request.Header.Set("User-Agent", "codex_exec/9.9.9")
	resolution := resolve(t, resolver, request)
	if resolution.Task.CapabilityTier != llm.TierChat || resolution.Task.HarnessSessionID != "" {
		t.Fatalf("unknown Codex version gained task affinity: %+v", resolution.Task)
	}
	if !stablePattern.MatchString(string(resolution.IdempotencyKey)) {
		t.Fatalf("unknown Codex version lost stable retry key: %q", resolution.IdempotencyKey)
	}
}

func TestCodexMalformedTurnMetadataFailsClosed(t *testing.T) {
	resolver := newResolver(t)
	request := codexRequest("caller-a", `{"model":"human","input":"fix it"}`,
		`{"request_kind":"turn","turn_id":"not-a-uuid"}`)
	if _, err := resolver.ResolveRequest(context.Background(), request); err == nil {
		t.Fatal("malformed Codex turn metadata must fail closed")
	}
}

func TestCodexExplicitKeyWins(t *testing.T) {
	resolver := newResolver(t)
	request := codexRequest("caller-a", `{}`, `{not-json`)
	request.Header.Set("Idempotency-Key", "caller-key")
	resolution := resolve(t, resolver, request)
	if resolution.IdempotencyKey != "caller-key" {
		t.Fatalf("Codex explicit key = %q", resolution.IdempotencyKey)
	}
	if resolution.Task.CapabilityTier != llm.TierChat {
		t.Fatalf("malformed advisory metadata gained affinity: %+v", resolution.Task)
	}
}

func TestCodexExplicitKeyPreservesValidatedAffinity(t *testing.T) {
	resolver := newResolver(t)
	request := codexRequest("caller-a", codexToolBody,
		`{"request_kind":"turn","turn_id":"`+codexTurnID+`"}`)
	request.Header.Set("Idempotency-Key", "caller-key")
	resolution := resolve(t, resolver, request)
	if resolution.IdempotencyKey != "caller-key" || resolution.Task.CapabilityTier != llm.TierRemoteTools ||
		resolution.Task.HarnessSessionID != codexTurnID {
		t.Fatalf("Codex explicit-key affinity = %+v key=%q", resolution.Task, resolution.IdempotencyKey)
	}
}

func TestOpenCodeKeyIsStableAcrossReserialization(t *testing.T) {
	resolver := newResolver(t)
	// Same semantic body, different top-level key order (a client re-serializing on
	// retry). The derived key must be identical so the retry dedups.
	ordered := `{"model":"human","stream":true,"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"fix the bug"}]}`
	shuffled := `{"messages":[{"role":"user","content":"fix the bug"}],"tools":[{"function":{"parameters":{"type":"object"},"name":"bash"},"type":"function"}],"stream":true,"model":"human"}`

	first := resolve(t, resolver, request("caller-a", opencodeHeader("ses_abc123"), ordered))
	second := resolve(t, resolver, request("caller-a", opencodeHeader("ses_abc123"), shuffled))
	if first.IdempotencyKey != second.IdempotencyKey {
		t.Fatalf("re-serialized body produced different keys:\n %q\n %q", first.IdempotencyKey, second.IdempotencyKey)
	}
}

func TestOpenCodeDistinctTurnsGetDistinctKeys(t *testing.T) {
	resolver := newResolver(t)
	turnOne := `{"model":"human","tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"first"}]}`
	turnTwo := `{"model":"human","tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}]}`

	first := resolve(t, resolver, request("caller-a", opencodeHeader("ses_abc123"), turnOne))
	second := resolve(t, resolver, request("caller-a", opencodeHeader("ses_abc123"), turnTwo))
	if first.IdempotencyKey == second.IdempotencyKey {
		t.Fatalf("distinct turns collapsed to one key %q", first.IdempotencyKey)
	}
}

func TestOpenCodeDifferentSessionsGetDifferentKeys(t *testing.T) {
	resolver := newResolver(t)
	first := resolve(t, resolver, request("caller-a", opencodeHeader("ses_one"), toolBody))
	second := resolve(t, resolver, request("caller-a", opencodeHeader("ses_two"), toolBody))
	if first.IdempotencyKey == second.IdempotencyKey {
		t.Fatal("same body in different sessions must not share a key")
	}
}

func TestExplicitKeyWinsOverDerivation(t *testing.T) {
	resolver := newResolver(t)
	header := opencodeHeader("ses_abc123")
	header.Set("Idempotency-Key", "caller-supplied-key")
	resolution := resolve(t, resolver, request("caller-a", header, toolBody))
	if resolution.IdempotencyKey != "caller-supplied-key" {
		t.Fatalf("explicit key = %q, want caller-supplied-key", resolution.IdempotencyKey)
	}
	if resolution.Task.CapabilityTier != llm.TierWorkspace {
		t.Fatalf("explicit key must not change tier: %q", resolution.Task.CapabilityTier)
	}
}

func TestOpenCodeAuxiliaryRequestDowngradesToChat(t *testing.T) {
	resolver := newResolver(t)
	// OpenCode title/summary generation: same UA + session, but no tools.
	auxiliary := `{"model":"human","messages":[{"role":"user","content":"summarize"}]}`
	resolution := resolve(t, resolver, request("caller-a", opencodeHeader("ses_abc123"), auxiliary))
	if resolution.Task.CapabilityTier != llm.TierChat {
		t.Fatalf("auxiliary tier = %q, want chat", resolution.Task.CapabilityTier)
	}
	if resolution.Task.HarnessSessionID != "" || resolution.Task.WorkspaceKey != "" {
		t.Fatalf("chat downgrade leaked affinity: %+v", resolution.Task)
	}
}

func TestNonOpenCodeRequestIsChat(t *testing.T) {
	resolver := newResolver(t)
	header := http.Header{}
	header.Set("User-Agent", "curl/8.0")
	resolution := resolve(t, resolver, request("caller-a", header, toolBody))
	if resolution.Task.CapabilityTier != llm.TierChat {
		t.Fatalf("non-opencode tier = %q, want chat", resolution.Task.CapabilityTier)
	}
	if !stablePattern.MatchString(string(resolution.IdempotencyKey)) {
		t.Fatalf("synthesized key %q is not a stable key", resolution.IdempotencyKey)
	}
}

func TestChatRandomKeysAreDistinct(t *testing.T) {
	resolver := newResolver(t)
	header := http.Header{}
	header.Set("User-Agent", "curl/8.0")
	first := resolve(t, resolver, request("caller-a", header, toolBody))
	second := resolve(t, resolver, request("caller-a", header, toolBody))
	if first.IdempotencyKey == second.IdempotencyKey {
		t.Fatal("two distinct chat requests must get distinct random keys")
	}
}

func TestMalformedSessionFailsClosed(t *testing.T) {
	resolver := newResolver(t)
	header := http.Header{}
	header.Set("User-Agent", opencodeUA)
	header.Add("X-Session-Id", "ses_one")
	header.Add("X-Session-Id", "ses_two")
	if _, err := resolver.ResolveRequest(context.Background(), request("caller-a", header, toolBody)); err == nil {
		t.Fatal("two X-Session-Id values must fail closed, not silently pick one")
	}
}

func TestSessionAffinityMustMatch(t *testing.T) {
	resolver := newResolver(t)
	header := opencodeHeader("ses_abc123")
	header.Set("X-Session-Affinity", "ses_different")
	if _, err := resolver.ResolveRequest(context.Background(), request("caller-a", header, toolBody)); err == nil {
		t.Fatal("mismatched X-Session-Affinity must fail closed")
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := harnessresolver.New(harnessresolver.Config{
		ExecAllowed: true,
	}); err == nil {
		t.Fatal("missing WorkspaceKey must error")
	}
	if _, err := harnessresolver.New(harnessresolver.Config{WorkspaceKey: "ws"}); err != nil {
		t.Fatal(err)
	}
}
