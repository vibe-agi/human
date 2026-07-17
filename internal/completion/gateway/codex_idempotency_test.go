package gateway

import (
	"net/http"
	"strings"
	"testing"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
)

const (
	testCodexTurnID     = "0198f3ce-6a62-7c4b-8e31-1a2b3c4d5e6f"
	testNextCodexTurnID = "0198f3d0-81a7-735c-9a42-2b3c4d5e6f70"
)

// This is a redacted fixture of the metadata shape observed from a real Codex
// Responses request. Values that can identify an installation, session, or
// workspace are synthetic; the field names and JSON types match the probe.
func testCodexMetadata(turnID string) string {
	return `{"installation_id":"00000000-0000-4000-8000-000000000001","session_id":"session-redacted","thread_id":"thread-redacted","turn_id":"` + turnID + `","window_id":"window-redacted","request_kind":"turn","thread_source":"user","sandbox":"seatbelt","turn_started_at_unix_ms":1770000000000,"workspaces":{"/workspace/example":{"remote":null}}}`
}

func testResponsesCanonical(text string) canonical.Request {
	return canonical.Request{
		Dialect: canonical.DialectResponses,
		Model:   "human-expert",
		Stream:  true,
		Messages: []canonical.Message{{
			Role:   canonical.RoleUser,
			Blocks: []canonical.Block{{Type: canonical.BlockText, Text: text}},
		}},
	}
}

func testCodexHeaders(metadata string) http.Header {
	header := make(http.Header)
	header.Set(headerCodexTurnMetadata, metadata)
	return header
}

func resolveTestCodexKey(
	t *testing.T,
	header http.Header,
	request canonical.Request,
	body []byte,
) string {
	t.Helper()
	key, err := resolveRequestIdempotencyKey(
		"", "caller-1", completion.TierChat, header, "codex_exec/0.144.0", request, body,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, codexDerivedIdempotencyPrefix) {
		t.Fatalf("derived key = %q", key)
	}
	return key
}

func TestCodexDerivedIdempotencyUsesJSONSemanticsAndEveryWireField(t *testing.T) {
	request := testResponsesCanonical("inspect retries")
	header := testCodexHeaders(testCodexMetadata(testCodexTurnID))
	firstBody := []byte(`{
      "model":"human-expert",
      "stream":true,
      "input":"inspect retries",
      "future_field":{"z":900719925474099312345,"a":[true,null,{"b":2,"a":1}]}
    }`)
	reorderedBody := []byte(`{"future_field":{"a":[true,null,{"a":1,"b":2}],"z":900719925474099312345},"input":"inspect retries","stream":true,"model":"human-expert"}`)

	first := resolveTestCodexKey(t, header, request, firstBody)
	reordered := resolveTestCodexKey(t, header, request, reorderedBody)
	if first != reordered {
		t.Fatalf("field-order-only change produced different keys: %q != %q", first, reordered)
	}

	unknownChanged := []byte(`{"future_field":{"a":[true,null,{"a":1,"b":3}],"z":900719925474099312345},"input":"inspect retries","stream":true,"model":"human-expert"}`)
	if changed := resolveTestCodexKey(t, header, request, unknownChanged); changed == first {
		t.Fatal("decoder-ignored wire field change reused the derived key")
	}

	largeNumberChanged := []byte(`{"future_field":{"a":[true,null,{"a":1,"b":2}],"z":900719925474099312346},"input":"inspect retries","stream":true,"model":"human-expert"}`)
	if changed := resolveTestCodexKey(t, header, request, largeNumberChanged); changed == first {
		t.Fatal("large JSON number change was lost through floating-point normalization")
	}
}

func TestCodexDerivedIdempotencySeparatesTurnsCallersAndToolLoops(t *testing.T) {
	firstRequest := testResponsesCanonical("first tool-loop request")
	firstBody := []byte(`{"model":"human-expert","stream":true,"input":"first tool-loop request"}`)
	firstHeader := testCodexHeaders(testCodexMetadata(testCodexTurnID))
	first := resolveTestCodexKey(t, firstHeader, firstRequest, firstBody)

	continuedRequest := testResponsesCanonical("tool result continuation")
	continuedBody := []byte(`{"model":"human-expert","stream":true,"input":"tool result continuation"}`)
	continued := resolveTestCodexKey(t, firstHeader, continuedRequest, continuedBody)
	if continued == first {
		t.Fatal("different canonical/body digest in one Codex turn reused a tool-loop key")
	}

	nextTurn := resolveTestCodexKey(
		t, testCodexHeaders(testCodexMetadata(testNextCodexTurnID)), firstRequest, firstBody,
	)
	if nextTurn == first {
		t.Fatal("next Codex turn reused the previous turn key")
	}

	otherCaller, err := resolveRequestIdempotencyKey(
		"", "caller-2", completion.TierChat, firstHeader, "codex_exec/0.144.0", firstRequest, firstBody,
	)
	if err != nil {
		t.Fatal(err)
	}
	if otherCaller == first {
		t.Fatal("authenticated caller identity was omitted from derived key")
	}
}

func TestExplicitIdempotencyKeyAlwaysWins(t *testing.T) {
	header := testCodexHeaders(`{"request_kind":"turn","turn_id":"not-a-uuid","turn_id":"duplicate"}`)
	key, err := resolveRequestIdempotencyKey(
		"caller-owned-key", "caller-1", completion.TierRemoteTools, header,
		"not-codex", canonical.Request{}, []byte(`not JSON`),
	)
	if err != nil || key != "caller-owned-key" {
		t.Fatalf("explicit key = %q, %v", key, err)
	}
}

func TestCodexAutoIdempotencyDoesNotExpandBeyondResponsesChat(t *testing.T) {
	validHeader := testCodexHeaders(testCodexMetadata(testCodexTurnID))
	validBody := []byte(`{"model":"human-expert","stream":true,"input":"scope"}`)
	tests := []struct {
		name      string
		callerID  string
		tier      completion.CapabilityTier
		request   canonical.Request
		header    http.Header
		userAgent string
	}{
		{name: "remote tools", callerID: "caller-1", tier: completion.TierRemoteTools, request: testResponsesCanonical("scope"), header: validHeader, userAgent: "codex_exec/0.144.0"},
		{name: "workspace", callerID: "caller-1", tier: completion.TierWorkspace, request: testResponsesCanonical("scope"), header: validHeader, userAgent: "codex_exec/0.144.0"},
		{name: "OpenAI Chat", callerID: "caller-1", tier: completion.TierChat, request: canonical.Request{Dialect: canonical.DialectOpenAIChat}, header: validHeader, userAgent: "codex_exec/0.144.0"},
		{name: "Anthropic", callerID: "caller-1", tier: completion.TierChat, request: canonical.Request{Dialect: canonical.DialectAnthropic}, header: validHeader, userAgent: "codex_exec/0.144.0"},
		{name: "unrecognized user agent", callerID: "caller-1", tier: completion.TierChat, request: testResponsesCanonical("scope"), header: validHeader, userAgent: "OpenAI/Python"},
		{name: "missing metadata", callerID: "caller-1", tier: completion.TierChat, request: testResponsesCanonical("scope"), header: make(http.Header), userAgent: "codex_exec/0.144.0"},
		{name: "missing authenticated caller", tier: completion.TierChat, request: testResponsesCanonical("scope"), header: validHeader, userAgent: "codex_exec/0.144.0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := resolveRequestIdempotencyKey(
				"", test.callerID, test.tier, test.header, test.userAgent, test.request, validBody,
			)
			if err != nil || key != "" {
				t.Fatalf("out-of-scope resolution = %q, %v", key, err)
			}
		})
	}
}

func TestNonTurnCodexMetadataSafelyUsesRandomBasicIdentity(t *testing.T) {
	for _, metadata := range []string{
		`{}`,
		`{"request_kind":"compact","turn_id":"not-a-uuid"}`,
		`{"request_kind":"background","turn_id":null}`,
	} {
		key, err := resolveRequestIdempotencyKey(
			"", "caller-1", completion.TierChat, testCodexHeaders(metadata),
			"codex_exec/0.144.0", testResponsesCanonical("scope"),
			[]byte(`{"model":"human-expert","stream":true,"input":"scope"}`),
		)
		if err != nil || key != "" {
			t.Fatalf("non-turn metadata %q = %q, %v", metadata, key, err)
		}
	}
}

func TestRecognizedCodexTurnRejectsAmbiguousIdentity(t *testing.T) {
	validBody := []byte(`{"model":"human-expert","stream":true,"input":"scope"}`)
	request := testResponsesCanonical("scope")
	oversized := strings.Repeat("x", maxCodexTurnMetadataBytes+1)
	multipleHeaders := testCodexHeaders(testCodexMetadata(testCodexTurnID))
	multipleHeaders.Add(headerCodexTurnMetadata, testCodexMetadata(testCodexTurnID))
	tests := []struct {
		name   string
		header http.Header
		body   []byte
	}{
		{name: "empty header", header: testCodexHeaders(""), body: validBody},
		{name: "oversized header", header: testCodexHeaders(oversized), body: validBody},
		{name: "multiple header values", header: multipleHeaders, body: validBody},
		{name: "malformed metadata", header: testCodexHeaders(`{"request_kind":`), body: validBody},
		{name: "metadata is array", header: testCodexHeaders(`[{"request_kind":"turn"}]`), body: validBody},
		{name: "metadata duplicate top level", header: testCodexHeaders(`{"request_kind":"turn","turn_id":"` + testCodexTurnID + `","turn_id":"` + testNextCodexTurnID + `"}`), body: validBody},
		{name: "metadata duplicate nested", header: testCodexHeaders(`{"request_kind":"turn","turn_id":"` + testCodexTurnID + `","workspaces":{"x":{"remote":1,"remote":2}}}`), body: validBody},
		{name: "request kind not string", header: testCodexHeaders(`{"request_kind":1,"turn_id":"` + testCodexTurnID + `"}`), body: validBody},
		{name: "missing turn id", header: testCodexHeaders(`{"request_kind":"turn"}`), body: validBody},
		{name: "invalid turn id", header: testCodexHeaders(`{"request_kind":"turn","turn_id":"not-a-uuid"}`), body: validBody},
		{name: "nil turn id", header: testCodexHeaders(`{"request_kind":"turn","turn_id":"00000000-0000-0000-0000-000000000000"}`), body: validBody},
		{name: "noncanonical uppercase turn id", header: testCodexHeaders(`{"request_kind":"turn","turn_id":"0198F3CE-6A62-7C4B-8E31-1A2B3C4D5E6F"}`), body: validBody},
		{name: "duplicate request body key", header: testCodexHeaders(testCodexMetadata(testCodexTurnID)), body: []byte(`{"model":"human-expert","stream":true,"input":"scope","nested":{"value":1,"value":2}}`)},
		{name: "multiple request body values", header: testCodexHeaders(testCodexMetadata(testCodexTurnID)), body: []byte(`{"model":"human-expert","stream":true,"input":"scope"} {}`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := resolveRequestIdempotencyKey(
				"", "caller-1", completion.TierChat, test.header,
				"codex_exec/0.144.0", request, test.body,
			)
			if err == nil || key != "" {
				t.Fatalf("ambiguous identity resolution = %q, %v", key, err)
			}
		})
	}
}

func TestCodexSemanticJSONRejectsExcessiveNesting(t *testing.T) {
	payload := strings.Repeat(`[`, maxCodexJSONNestingDepth+1) + `null` +
		strings.Repeat(`]`, maxCodexJSONNestingDepth+1)
	if _, err := jsonSemanticDigest([]byte(payload)); err == nil {
		t.Fatal("excessively nested Codex request body was accepted")
	}
}
