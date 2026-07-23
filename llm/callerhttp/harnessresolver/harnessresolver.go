// Package harnessresolver is HumanLLM's basic caller-side RequestResolver: it
// maps the exact session identity emitted by recognized coding harnesses onto
// the transport-neutral llm.TaskContext, and derives a stable idempotency key
// for a harness turn so a dropped-socket retry dedups instead of forking work.
//
// It is a basic default, not the product: the product is the
// callerhttp.RequestResolver port, which an embedder can implement against their
// own trusted session/workspace system. This implementation recognizes the exact
// Claude Code and Codex remote-tool profiles, the exact OpenCode workspace
// profile, and treats everything else as a fresh Chat turn.
//
// Task identity is deliberately NOT derived here: the resolver supplies the full
// harness affinity tuple and omits TaskID, so llm.Service's own FindOpenTask
// affinity resolution reuses the session's one open task across a
// clarification/tool-result loop and opens a new task only once the previous turn
// is terminal. That is the single source of truth for task continuation.
package harnessresolver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/callerhttp"
)

const (
	headerSessionID       = "X-Session-Id"
	headerSessionAffinity = "X-Session-Affinity"

	openCodeHarnessID = "opencode"
	openCodeVersion   = "1.17.18"
	openCodeUAPrefix  = "opencode/" + openCodeVersion + " "
	openCodeKeyDomain = "human:auto:opencode-turn:v1"
	openCodeKeyPrefix = "auto:opencode-turn:v1:"

	headerClaudeSessionID = "X-Claude-Code-Session-Id"
	claudeUAPrefix        = "claude-cli/"
	claudeHarnessID       = "claude-code"
	claudeToolsVersion    = "2.1.217"
	claudeMessagesCodec   = llm.CodecID("anthropic.messages")
	claudeKeyDomain       = "human:auto:claude-turn:v1"
	claudeKeyPrefix       = "auto:claude-turn:v1:"

	headerCodexTurnMetadata   = "X-Codex-Turn-Metadata"
	codexUAPrefix             = "codex_exec/"
	codexHarnessID            = "codex"
	codexWorkspaceVersion     = "0.145.0"
	codexResponsesCodec       = llm.CodecID("openai.responses")
	codexKeyDomain            = "human:auto:codex-turn:v1"
	codexKeyPrefix            = "auto:codex-turn:v1:"
	maxCodexTurnMetadataBytes = 16 << 10

	maxJSONNestingDepth = 256
)

// stableKeyPattern is the exact llm/callerhttp stable-identity shape a resolved
// idempotency key must satisfy. Derived keys are built to fit it.
var stableKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// Config binds the basic resolver to one logical project. WorkspaceKey is the
// stable affinity namespace a session's tasks live in; no caller filesystem
// path crosses this boundary. ExecAllowed permits shell tools.
type Config struct {
	WorkspaceKey string
	ExecAllowed  bool
}

// Resolver is the basic RequestResolver implementation.
type Resolver struct {
	workspaceKey string
	execAllowed  bool
}

var _ callerhttp.RequestResolver = (*Resolver)(nil)

// New validates the workspace binding and returns a ready resolver.
func New(config Config) (*Resolver, error) {
	if !stableKeyPattern.MatchString(config.WorkspaceKey) {
		return nil, errors.New("harnessresolver: WorkspaceKey must be a stable key")
	}
	return &Resolver{
		workspaceKey: config.WorkspaceKey,
		execAllowed:  config.ExecAllowed,
	}, nil
}

// ResolveRequest classifies the request and returns its durable retry key plus
// task affinity.
func (resolver *Resolver) ResolveRequest(
	ctx context.Context,
	request callerhttp.ResolutionRequest,
) (callerhttp.Resolution, error) {
	if ctx == nil {
		return callerhttp.Resolution{}, fmt.Errorf("%w: context is required", callerhttp.ErrResolution)
	}
	if err := ctx.Err(); err != nil {
		return callerhttp.Resolution{}, err
	}
	if codex, key, turnID, version, err := resolver.classifyCodex(request); err != nil {
		return callerhttp.Resolution{}, err
	} else if codex {
		task := llm.TaskContext{CapabilityTier: llm.TierChat}
		// Codex 0.145.0 exposes one canonical turn UUID across the initial
		// Responses request and its tool-output continuation. Unknown versions
		// retain safe Chat retry dedup until their exact wire contract is
		// validated. An exact custom/freeform apply_patch declaration upgrades
		// the request from RemoteTools to Workspace.
		if turnID != "" && version == codexWorkspaceVersion && bodyDeclaresTools(request.Body) {
			tier := llm.TierRemoteTools
			if codexWorkspaceToolsDeclared(request.Body) {
				tier = llm.TierWorkspace
			}
			task = llm.TaskContext{
				CapabilityTier: tier, WorkspaceKey: resolver.workspaceKey,
				HarnessID: codexHarnessID, HarnessVersion: version, HarnessSessionID: turnID,
				ExecAllowed: resolver.execAllowed,
			}
		}
		return callerhttp.Resolution{
			IdempotencyKey: key,
			Task:           task,
		}, nil
	}

	if claude, key, session, version, err := resolver.classifyClaude(request); err != nil {
		return callerhttp.Resolution{}, err
	} else if claude {
		task := llm.TaskContext{CapabilityTier: llm.TierChat}
		if version == claudeToolsVersion && session != "" && bodyDeclaresTools(request.Body) {
			tier := llm.TierRemoteTools
			if claudeWorkspaceToolsDeclared(request.Body) {
				tier = llm.TierWorkspace
			}
			task = llm.TaskContext{
				CapabilityTier: tier, WorkspaceKey: resolver.workspaceKey,
				HarnessID: claudeHarnessID, HarnessVersion: version, HarnessSessionID: session,
				ExecAllowed: resolver.execAllowed,
			}
		}
		return callerhttp.Resolution{IdempotencyKey: key, Task: task}, nil
	}

	workspace, session, err := resolver.classifyOpenCode(request)
	if err != nil {
		return callerhttp.Resolution{}, err
	}
	if workspace {
		key, keyErr := resolver.turnKey(request, session)
		if keyErr != nil {
			return callerhttp.Resolution{}, keyErr
		}
		return callerhttp.Resolution{
			IdempotencyKey: key,
			Task: llm.TaskContext{
				CapabilityTier:   llm.TierWorkspace,
				WorkspaceKey:     resolver.workspaceKey,
				HarnessID:        openCodeHarnessID,
				HarnessVersion:   openCodeVersion,
				HarnessSessionID: session,
				ExecAllowed:      resolver.execAllowed,
			},
		}, nil
	}

	// Chat: an explicit caller key wins; otherwise a distinct random key keeps two
	// separate chat requests separate (a chat turn owns a fresh task, so a body
	// hash would wrongly collapse two identical questions into one answer).
	key, keyErr := resolver.chatKey(request)
	if keyErr != nil {
		return callerhttp.Resolution{}, keyErr
	}
	return callerhttp.Resolution{IdempotencyKey: key, Task: llm.TaskContext{CapabilityTier: llm.TierChat}}, nil
}

// codexWorkspaceToolsDeclared recognizes Codex 0.145.0's Responses custom
// apply_patch contract. The grammar text is deliberately not pinned byte for
// byte: its parser-safe envelope is the authority boundary, while harmless
// grammar refinements within the pinned CLI version must not disable file
// delivery.
func codexWorkspaceToolsDeclared(body []byte) bool {
	var probe struct {
		Tools []struct {
			Type         string          `json:"type"`
			Name         string          `json:"name"`
			Format       json.RawMessage `json:"format"`
			DeferLoading *bool           `json:"defer_loading"`
		} `json:"tools"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return false
	}
	for _, tool := range probe.Tools {
		if tool.Type != "custom" || tool.Name != "apply_patch" ||
			(tool.DeferLoading != nil && *tool.DeferLoading) {
			continue
		}
		var format struct {
			Type       string `json:"type"`
			Syntax     string `json:"syntax"`
			Definition string `json:"definition"`
		}
		if json.Unmarshal(tool.Format, &format) == nil &&
			format.Type == "grammar" && format.Syntax == "lark" &&
			strings.Contains(format.Definition, "*** Begin Patch") &&
			strings.Contains(format.Definition, "*** End Patch") &&
			strings.Contains(format.Definition, "*** Update File: ") &&
			strings.Contains(format.Definition, "*** Add File: ") &&
			strings.Contains(format.Definition, "*** Delete File: ") {
			return true
		}
	}
	return false
}

// claudeWorkspaceToolsDeclared recognizes the exact native file schemas
// captured from Claude Code 2.1.217. Descriptions and $schema annotations are
// intentionally ignored, but property names/types, required fields and the
// closed-object boundary must match before the resolver grants Workspace.
func claudeWorkspaceToolsDeclared(body []byte) bool {
	var probe struct {
		Tools []struct {
			Name        string          `json:"name"`
			InputSchema json.RawMessage `json:"input_schema"`
		} `json:"tools"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return false
	}
	want := map[string]toolSchemaShape{
		"Write": {
			properties: map[string]string{"file_path": "string", "content": "string"},
			required:   []string{"file_path", "content"},
		},
		"Edit": {
			properties: map[string]string{
				"file_path": "string", "old_string": "string", "new_string": "string", "replace_all": "boolean",
			},
			required: []string{"file_path", "old_string", "new_string"},
		},
	}
	for _, tool := range probe.Tools {
		shape, needed := want[tool.Name]
		if !needed || !matchesClosedToolSchema(tool.InputSchema, shape) {
			continue
		}
		delete(want, tool.Name)
	}
	return len(want) == 0
}

type toolSchemaShape struct {
	properties map[string]string
	required   []string
}

func matchesClosedToolSchema(raw json.RawMessage, want toolSchemaShape) bool {
	var schema struct {
		Type       string `json:"type"`
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
		Required             []string `json:"required"`
		AdditionalProperties *bool    `json:"additionalProperties"`
	}
	if json.Unmarshal(raw, &schema) != nil || schema.Type != "object" ||
		schema.AdditionalProperties == nil || *schema.AdditionalProperties ||
		len(schema.Properties) != len(want.properties) || len(schema.Required) != len(want.required) {
		return false
	}
	for name, kind := range want.properties {
		property, exists := schema.Properties[name]
		if !exists || property.Type != kind {
			return false
		}
	}
	required := make(map[string]struct{}, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = struct{}{}
	}
	for _, name := range want.required {
		if _, exists := required[name]; !exists {
			return false
		}
	}
	return len(required) == len(schema.Required)
}

// classifyClaude recognizes Claude Code's exact Anthropic Messages request
// identity. Claude 2.1.217 emits the same canonical session UUID in both the
// X-Claude-Code-Session-Id header and metadata.user_id's embedded JSON. Requiring
// both copies to agree prevents advisory body metadata from silently granting a
// remote-tool affinity. Unknown versions keep request retry dedup but receive no
// task affinity until their wire contract is captured and validated.
func (resolver *Resolver) classifyClaude(
	request callerhttp.ResolutionRequest,
) (bool, llm.IdempotencyKey, string, string, error) {
	if request.Route.CodecID != claudeMessagesCodec {
		return false, "", "", "", nil
	}
	userAgents := request.Header.Values("User-Agent")
	if len(userAgents) != 1 {
		return false, "", "", "", nil
	}
	userAgentToken, _, _ := strings.Cut(strings.TrimSpace(userAgents[0]), " ")
	if !strings.HasPrefix(userAgentToken, claudeUAPrefix) {
		return false, "", "", "", nil
	}
	version := strings.TrimPrefix(userAgentToken, claudeUAPrefix)
	if version == "" || !stableKeyPattern.MatchString(version) {
		return true, "", "", "", fmt.Errorf("%w: Claude User-Agent version is invalid", callerhttp.ErrResolution)
	}

	sessions := request.Header.Values(headerClaudeSessionID)
	if len(sessions) != 1 {
		return true, "", "", version, fmt.Errorf("%w: exact Claude profile requires one %s", callerhttp.ErrResolution, headerClaudeSessionID)
	}
	session, err := canonicalUUID(sessions[0])
	if err != nil {
		return true, "", "", version, fmt.Errorf("%w: %s must be a canonical non-nil UUID", callerhttp.ErrResolution, headerClaudeSessionID)
	}
	bodySession, err := claudeBodySession(request.Body)
	if err != nil {
		return true, "", "", version, err
	}
	if bodySession != session {
		return true, "", "", version, fmt.Errorf("%w: Claude metadata session must match %s", callerhttp.ErrResolution, headerClaudeSessionID)
	}

	if explicit, ok, explicitErr := explicitKey(request.Header); explicitErr != nil {
		return true, "", "", version, explicitErr
	} else if ok {
		return true, explicit, session, version, nil
	}
	bodyDigest, err := jsonSemanticDigest(request.Body)
	if err != nil {
		return true, "", "", version, fmt.Errorf("%w: Claude request body is not unambiguous JSON", callerhttp.ErrResolution)
	}
	digest := sha256.New()
	writeDigestField(digest, claudeKeyDomain)
	writeDigestField(digest, string(request.CallerID))
	writeDigestField(digest, string(request.Route.CodecID))
	writeDigestField(digest, version)
	writeDigestField(digest, session)
	writeDigestField(digest, hex.EncodeToString(bodyDigest[:]))
	return true, llm.IdempotencyKey(claudeKeyPrefix + hex.EncodeToString(digest.Sum(nil))), session, version, nil
}

func claudeBodySession(body []byte) (string, error) {
	decoded, err := decodeUniqueJSON(body)
	if err != nil {
		return "", fmt.Errorf("%w: Claude request body is not unambiguous JSON", callerhttp.ErrResolution)
	}
	top, ok := decoded.(map[string]any)
	if !ok {
		return "", fmt.Errorf("%w: Claude request body must be a JSON object", callerhttp.ErrResolution)
	}
	metadata, ok := top["metadata"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("%w: exact Claude profile requires metadata", callerhttp.ErrResolution)
	}
	userID, ok := metadata["user_id"].(string)
	if !ok || userID == "" {
		return "", fmt.Errorf("%w: exact Claude profile requires metadata.user_id", callerhttp.ErrResolution)
	}
	embedded, err := decodeUniqueJSON([]byte(userID))
	if err != nil {
		return "", fmt.Errorf("%w: Claude metadata.user_id is not unambiguous JSON", callerhttp.ErrResolution)
	}
	identity, ok := embedded.(map[string]any)
	if !ok {
		return "", fmt.Errorf("%w: Claude metadata.user_id must encode a JSON object", callerhttp.ErrResolution)
	}
	session, ok := identity["session_id"].(string)
	if !ok {
		return "", fmt.Errorf("%w: Claude metadata.user_id requires session_id", callerhttp.ErrResolution)
	}
	canonical, err := canonicalUUID(session)
	if err != nil {
		return "", fmt.Errorf("%w: Claude metadata session_id must be a canonical non-nil UUID", callerhttp.ErrResolution)
	}
	return canonical, nil
}

func canonicalUUID(value string) (string, error) {
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil || parsed.String() != value {
		return "", errors.New("not a canonical non-nil UUID")
	}
	return value, nil
}

// classifyCodex recognizes the retry identity emitted by Codex's Responses
// client. A validated version and turn UUID let the single-workspace reference
// resolver construct task affinity; unknown versions receive request-level
// retry dedup only. An explicit Idempotency-Key wins without turning malformed
// advisory metadata into authority. Without an explicit key, malformed or
// ambiguous turn metadata fails closed instead of silently forking work.
func (resolver *Resolver) classifyCodex(
	request callerhttp.ResolutionRequest,
) (bool, llm.IdempotencyKey, string, string, error) {
	if request.Route.CodecID != codexResponsesCodec {
		return false, "", "", "", nil
	}
	userAgents := request.Header.Values("User-Agent")
	if len(userAgents) != 1 {
		return false, "", "", "", nil
	}
	userAgentToken, _, _ := strings.Cut(strings.TrimSpace(userAgents[0]), " ")
	if !strings.HasPrefix(userAgentToken, codexUAPrefix) {
		return false, "", "", "", nil
	}
	version := strings.TrimPrefix(userAgentToken, codexUAPrefix)
	if version == "" || !stableKeyPattern.MatchString(version) {
		return true, "", "", "", fmt.Errorf("%w: Codex User-Agent version is invalid", callerhttp.ErrResolution)
	}
	explicit, explicitPresent, err := explicitKey(request.Header)
	if err != nil {
		return true, "", "", version, err
	}
	values := request.Header.Values(headerCodexTurnMetadata)
	if len(values) == 0 {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		key, err := randomKey()
		return true, key, "", version, err
	}
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > maxCodexTurnMetadataBytes {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		return true, "", "", version, fmt.Errorf("%w: Codex turn metadata must be one non-empty value of at most 16 KiB", callerhttp.ErrResolution)
	}
	decoded, err := decodeUniqueJSON([]byte(values[0]))
	if err != nil {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		return true, "", "", version, fmt.Errorf("%w: Codex turn metadata is not unambiguous JSON", callerhttp.ErrResolution)
	}
	metadata, ok := decoded.(map[string]any)
	if !ok {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		return true, "", "", version, fmt.Errorf("%w: Codex turn metadata must be a JSON object", callerhttp.ErrResolution)
	}
	kindValue, present := metadata["request_kind"]
	if !present {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		key, keyErr := randomKey()
		return true, key, "", version, keyErr
	}
	kind, ok := kindValue.(string)
	if !ok {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		return true, "", "", version, fmt.Errorf("%w: Codex request_kind must be a string", callerhttp.ErrResolution)
	}
	if kind != "turn" {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		key, keyErr := randomKey()
		return true, key, "", version, keyErr
	}
	turnID, ok := metadata["turn_id"].(string)
	parsed, parseErr := uuid.Parse(turnID)
	if !ok || parseErr != nil || parsed == uuid.Nil || parsed.String() != turnID {
		if explicitPresent {
			return true, explicit, "", version, nil
		}
		return true, "", "", version, fmt.Errorf("%w: Codex turn_id must be a canonical non-nil UUID", callerhttp.ErrResolution)
	}
	if explicitPresent {
		return true, explicit, turnID, version, nil
	}
	bodyDigest, err := jsonSemanticDigest(request.Body)
	if err != nil {
		return true, "", "", version, fmt.Errorf("%w: Codex request body is not unambiguous JSON", callerhttp.ErrResolution)
	}
	digest := sha256.New()
	writeDigestField(digest, codexKeyDomain)
	writeDigestField(digest, string(request.CallerID))
	writeDigestField(digest, string(request.Route.CodecID))
	writeDigestField(digest, version)
	writeDigestField(digest, turnID)
	writeDigestField(digest, hex.EncodeToString(bodyDigest[:]))
	return true, llm.IdempotencyKey(codexKeyPrefix + hex.EncodeToString(digest.Sum(nil))), turnID, version, nil
}

// classifyOpenCode reports whether this is an exact OpenCode workspace turn: the
// captured OpenCode User-Agent, exactly one valid X-Session-Id (optionally echoed
// by a matching X-Session-Affinity), and a tool-declaring body. Auxiliary OpenCode
// requests (title/summary generation, no tools) fall through to Chat. A malformed
// session identity on an OpenCode request fails closed rather than silently
// downgrading, so it can never quietly fork a new task under a fresh key.
func (resolver *Resolver) classifyOpenCode(request callerhttp.ResolutionRequest) (bool, string, error) {
	if !strings.HasPrefix(request.Header.Get("User-Agent"), openCodeUAPrefix) {
		return false, "", nil
	}
	sessions := request.Header.Values(headerSessionID)
	if len(sessions) == 0 {
		return false, "", fmt.Errorf("%w: exact OpenCode profile requires %s", callerhttp.ErrResolution, headerSessionID)
	}
	session := strings.TrimSpace(sessions[0])
	if len(sessions) != 1 || !stableKeyPattern.MatchString(session) {
		return false, "", fmt.Errorf("%w: %s must be exactly one stable key", callerhttp.ErrResolution, headerSessionID)
	}
	if affinity := request.Header.Values(headerSessionAffinity); len(affinity) > 1 ||
		(len(affinity) == 1 && strings.TrimSpace(affinity[0]) != session) {
		return false, "", fmt.Errorf("%w: %s must match %s", callerhttp.ErrResolution, headerSessionAffinity, headerSessionID)
	}
	if !bodyDeclaresTools(request.Body) {
		return false, session, nil // auxiliary OpenCode request → Chat
	}
	return true, session, nil
}

// bodyDeclaresTools reports whether the request body declares at least one tool.
func bodyDeclaresTools(body []byte) bool {
	var probe struct {
		Tools []json.RawMessage `json:"tools"`
	}
	return json.Unmarshal(body, &probe) == nil && len(probe.Tools) > 0
}

// turnKey derives the OpenCode turn's durable idempotency key. An explicit caller
// key always wins. The derived key binds the harness session plus a semantic
// digest of the body, so a transport retry (even one that re-serializes the JSON)
// dedups, while the next turn — whose history has grown — gets a fresh key.
func (resolver *Resolver) turnKey(request callerhttp.ResolutionRequest, session string) (llm.IdempotencyKey, error) {
	if explicit, ok, err := explicitKey(request.Header); err != nil {
		return "", err
	} else if ok {
		return explicit, nil
	}
	bodyDigest, err := jsonSemanticDigest(request.Body)
	if err != nil {
		return "", fmt.Errorf("%w: OpenCode request body is not unambiguous JSON", callerhttp.ErrResolution)
	}
	digest := sha256.New()
	writeDigestField(digest, openCodeKeyDomain)
	writeDigestField(digest, string(request.CallerID))
	writeDigestField(digest, resolver.workspaceKey)
	writeDigestField(digest, session)
	writeDigestField(digest, hex.EncodeToString(bodyDigest[:]))
	return llm.IdempotencyKey(openCodeKeyPrefix + hex.EncodeToString(digest.Sum(nil))), nil
}

// chatKey returns an explicit caller key or a fresh random one.
func (resolver *Resolver) chatKey(request callerhttp.ResolutionRequest) (llm.IdempotencyKey, error) {
	if explicit, ok, err := explicitKey(request.Header); err != nil {
		return "", err
	} else if ok {
		return explicit, nil
	}
	return randomKey()
}

// explicitKey returns a caller-supplied Idempotency-Key when present. A malformed
// value fails closed rather than being silently ignored (which would fork work).
func explicitKey(header http.Header) (llm.IdempotencyKey, bool, error) {
	values := header.Values(callerhttp.HeaderIdempotencyKey)
	if len(values) == 0 {
		return "", false, nil
	}
	if len(values) != 1 || !stableKeyPattern.MatchString(values[0]) {
		return "", false, fmt.Errorf("%w: %s must be a single stable key", callerhttp.ErrResolution, callerhttp.HeaderIdempotencyKey)
	}
	return llm.IdempotencyKey(values[0]), true, nil
}

// randomKey allocates a fresh opaque idempotency key.
func randomKey() (llm.IdempotencyKey, error) {
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", fmt.Errorf("%w: failed to allocate a request key", callerhttp.ErrResolution)
	}
	return llm.IdempotencyKey("req-" + hex.EncodeToString(raw[:])), nil
}

// writeDigestField length-prefixes each field so a digest cannot be forged by
// shifting bytes across field boundaries.
func writeDigestField(destination hash.Hash, value string) {
	var size [8]byte
	putUint64(size[:], uint64(len(value)))
	_, _ = destination.Write(size[:])
	_, _ = destination.Write([]byte(value))
}

func putUint64(destination []byte, value uint64) {
	for index := 7; index >= 0; index-- {
		destination[index] = byte(value)
		value >>= 8
	}
}

// jsonSemanticDigest is a re-serialization-stable digest of a JSON document: it
// decodes to a canonical value (sorted object keys, duplicate keys rejected,
// number precision retained) and hashes the canonical form, so two byte-different
// but semantically identical bodies hash equal.
func jsonSemanticDigest(payload []byte) ([sha256.Size]byte, error) {
	value, err := decodeUniqueJSON(payload)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	canonicalPayload, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(canonicalPayload), nil
}

// decodeUniqueJSON decodes exactly one JSON value while retaining json.Number
// precision and rejecting duplicate object keys at every nesting depth. A plain
// map unmarshal would silently let a later duplicate replace an earlier value,
// making a derived idempotency identity ambiguous.
func decodeUniqueJSON(payload []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	value, err := decodeUniqueJSONValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func decodeUniqueJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > maxJSONNestingDepth {
		return nil, errors.New("JSON nesting is too deep")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return nil, keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, errors.New("duplicate JSON object key")
			}
			value, valueErr := decodeUniqueJSONValue(decoder, depth+1)
			if valueErr != nil {
				return nil, valueErr
			}
			object[key] = value
		}
		if closing, closeErr := decoder.Token(); closeErr != nil {
			return nil, closeErr
		} else if closing != json.Delim('}') {
			return nil, errors.New("unterminated JSON object")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, valueErr := decodeUniqueJSONValue(decoder, depth+1)
			if valueErr != nil {
				return nil, valueErr
			}
			array = append(array, value)
		}
		if closing, closeErr := decoder.Token(); closeErr != nil {
			return nil, closeErr
		} else if closing != json.Delim(']') {
			return nil, errors.New("unterminated JSON array")
		}
		return array, nil
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}
