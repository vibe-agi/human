package callershim

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/workerproto"
)

const (
	headerCapabilityTier = "X-Human-Capability-Tier"
	headerCallerID       = "X-Human-Caller-Id"
	headerWorkspaceKey   = "X-Human-Workspace-Key"
	headerTaskID         = "X-Human-Task-Id"
	headerHarnessID      = "X-Human-Harness-Id"
	headerHarnessVersion = "X-Human-Harness-Version"
	headerWorkspaceRoot  = "X-Human-Workspace-Root"
	headerAllowExec      = "X-Human-Allow-Exec"
	headerIdempotencyKey = "Idempotency-Key"
)

type ServerConfig struct {
	GatewayURL  string
	CallerToken string
	ToolToken   string
	// CallerID and TaskID form the token-independent, durable execution-ledger
	// namespace. Reuse both with the same ledger after a process restart.
	CallerID      string
	WorkspaceKey  string
	WorkspaceRoot string
	TaskID        string
	AllowExec     bool
	MaxBodyBytes  int64
	HTTPClient    *http.Client
	Executor      *Executor
}

type Server struct {
	config  ServerConfig
	gateway *url.URL
	client  *http.Client
	handler http.Handler
}

func NewServer(config ServerConfig) (*Server, error) {
	gateway, err := url.Parse(strings.TrimSpace(config.GatewayURL))
	if err != nil || gateway.Scheme == "" || gateway.Host == "" {
		return nil, errors.New("absolute gateway URL is required")
	}
	if config.CallerToken == "" || config.ToolToken == "" || config.CallerID == "" ||
		config.WorkspaceKey == "" || config.WorkspaceRoot == "" || config.TaskID == "" || config.Executor == nil {
		return nil, errors.New("caller token, tool token, caller, workspace, task, root, and executor are required")
	}
	config.CallerID = strings.TrimSpace(config.CallerID)
	config.WorkspaceKey = strings.TrimSpace(config.WorkspaceKey)
	config.WorkspaceRoot = strings.TrimSpace(config.WorkspaceRoot)
	config.TaskID = strings.TrimSpace(config.TaskID)
	if err := validateToolIdentity(config, strings.TrimSpace(config.TaskID), "request_validation"); err != nil {
		return nil, fmt.Errorf("invalid caller shim identity: %w", err)
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = workerproto.MaxWireMessageBytes
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	server := &Server{config: config, gateway: gateway, client: client}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /internal/v1/tools/schema", server.toolSchema)
	mux.HandleFunc("POST /internal/v1/tools/execute", server.executeTool)
	mux.HandleFunc("POST /v1/chat/completions", server.proxyCompletion)
	mux.HandleFunc("POST /v1/messages", server.proxyCompletion)
	mux.HandleFunc("POST /v1/responses", server.proxyCompletion)
	server.handler = mux
	return server, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	server.handler.ServeHTTP(response, request)
}

func (*Server) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (server *Server) proxyCompletion(response http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, server.config.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(response, "request body exceeds the worker wire limit", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(response, "request body is unreadable", http.StatusBadRequest)
		}
		return
	}
	taskID := strings.TrimSpace(server.config.TaskID)
	if requestedTaskID := strings.TrimSpace(request.Header.Get(headerTaskID)); requestedTaskID != "" && requestedTaskID != taskID {
		http.Error(response, "stable remote-tools identity required: task_id does not match this shim", http.StatusPreconditionRequired)
		return
	}
	idempotencyKey := strings.TrimSpace(request.Header.Get(headerIdempotencyKey))
	if err := validateToolIdentity(server.config, taskID, idempotencyKey); err != nil {
		http.Error(response, "stable remote-tools identity required: "+err.Error(), http.StatusPreconditionRequired)
		return
	}
	target := *server.gateway
	target.Path = strings.TrimRight(server.gateway.Path, "/") + request.URL.Path
	target.RawQuery = request.URL.RawQuery
	upstream, err := http.NewRequestWithContext(request.Context(), request.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		http.Error(response, "failed to create upstream request", http.StatusInternalServerError)
		return
	}
	copyEndToEndHeaders(upstream.Header, request.Header)
	upstream.Header.Set("Authorization", "Bearer "+server.config.CallerToken)
	upstream.Header.Set(headerCapabilityTier, "remote_tools")
	upstream.Header.Set(headerCallerID, server.config.CallerID)
	upstream.Header.Set(headerWorkspaceKey, server.config.WorkspaceKey)
	upstream.Header.Set(headerTaskID, taskID)
	upstream.Header.Set(headerHarnessID, adapter.HumanShimID)
	upstream.Header.Set(headerHarnessVersion, adapter.HumanShimVersion)
	upstream.Header.Set(headerWorkspaceRoot, server.config.WorkspaceRoot)
	upstream.Header.Set(headerAllowExec, fmt.Sprintf("%t", server.config.AllowExec))
	upstream.Header.Set(headerIdempotencyKey, idempotencyKey)

	upstreamResponse, err := server.client.Do(upstream)
	if err != nil {
		http.Error(response, "gateway unavailable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upstreamResponse.Body.Close()
	copyEndToEndHeaders(response.Header(), upstreamResponse.Header)
	response.Header().Set(headerTaskID, taskID)
	response.Header().Set(headerIdempotencyKey, idempotencyKey)
	response.WriteHeader(upstreamResponse.StatusCode)
	flusher, _ := response.(http.Flusher)
	buffer := make([]byte, 32*1024)
	for {
		count, readErr := upstreamResponse.Body.Read(buffer)
		if count > 0 {
			if _, writeErr := response.Write(buffer[:count]); writeErr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			return
		}
	}
}

func (server *Server) executeTool(response http.ResponseWriter, request *http.Request) {
	if !bearerMatches(request.Header.Get("Authorization"), server.config.ToolToken) {
		http.Error(response, "tool token required", http.StatusUnauthorized)
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, server.config.MaxBodyBytes))
	decoder.DisallowUnknownFields()
	var toolRequest ToolRequest
	if err := decoder.Decode(&toolRequest); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(response, "tool request exceeds the worker wire limit", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(response, "invalid tool request: "+err.Error(), http.StatusBadRequest)
		}
		return
	}
	// A tool-call source is not trusted to select its ledger namespace. If
	// these body fields were honored, changing either would evade a replay.
	toolRequest.CallerID = strings.TrimSpace(server.config.CallerID)
	toolRequest.TaskID = strings.TrimSpace(server.config.TaskID)
	result, err := server.config.Executor.Execute(request.Context(), toolRequest)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrExecutionReplay) {
			status = http.StatusConflict
		} else if errors.Is(err, ErrExecutionPending) {
			status = http.StatusServiceUnavailable
		}
		writeJSON(response, status, ToolResponse{Content: err.Error(), IsError: true, ErrorCode: "execution_ledger_error"})
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (server *Server) toolSchema(response http.ResponseWriter, request *http.Request) {
	if !bearerMatches(request.Header.Get("Authorization"), server.config.ToolToken) {
		http.Error(response, "tool token required", http.StatusUnauthorized)
		return
	}
	profile := adapter.HumanShimProfile()
	writeJSON(response, http.StatusOK, map[string]any{
		"harness_id": profile.HarnessID, "harness_version": profile.HarnessVersion,
		"openai": humanShimOpenAITools(server.config.AllowExec),
	})
}

func humanShimOpenAITools(allowExec bool) []map[string]any {
	definitions := []struct {
		name        string
		description string
		properties  map[string]any
		required    []string
	}{
		{"human_read_file", "Read a file from /workspace and return its content hash.", map[string]any{"path": stringProperty()}, []string{"path"}},
		{"human_search", "Search literal text under /workspace.", map[string]any{"query": stringProperty(), "path": stringProperty(), "max_results": map[string]any{"type": "integer"}}, []string{"query"}},
		{"human_write_file", "CAS-create or replace a file.", map[string]any{"path": stringProperty(), "content": stringProperty(), "encoding": stringProperty(), "expected_sha256": stringProperty()}, []string{"path", "content", "expected_sha256"}},
		{"human_edit_file", "Replace exactly one text occurrence after a content-hash CAS check.", map[string]any{"path": stringProperty(), "old_string": stringProperty(), "new_string": stringProperty(), "expected_sha256": stringProperty()}, []string{"path", "old_string", "new_string", "expected_sha256"}},
		{"human_delete_file", "Delete a file after a content-hash CAS check.", map[string]any{"path": stringProperty(), "expected_sha256": stringProperty()}, []string{"path", "expected_sha256"}},
		{"human_rename_file", "Rename a file after a content-hash CAS check.", map[string]any{"from": stringProperty(), "to": stringProperty(), "expected_sha256": stringProperty()}, []string{"from", "to", "expected_sha256"}},
	}
	if allowExec {
		definitions = append(definitions, struct {
			name        string
			description string
			properties  map[string]any
			required    []string
		}{"human_exec", "Run a bounded command in the caller workspace when explicitly enabled.", map[string]any{"command": stringProperty(), "cwd": stringProperty(), "timeout_ms": map[string]any{"type": "integer"}}, []string{"command"}})
	}
	tools := make([]map[string]any, 0, len(definitions))
	for _, definition := range definitions {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name": definition.name, "description": definition.description,
				"parameters": map[string]any{
					"type": "object", "properties": definition.properties,
					"required": definition.required, "additionalProperties": false,
				},
			},
		})
	}
	return tools
}

func stringProperty() map[string]any { return map[string]any{"type": "string"} }

func bearerMatches(header, expected string) bool {
	if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return false
	}
	actual := strings.TrimSpace(header[len("Bearer "):])
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

var hopByHopHeaders = map[string]struct{}{
	"Connection": {}, "Proxy-Connection": {}, "Keep-Alive": {},
	"Proxy-Authenticate": {}, "Proxy-Authorization": {}, "Te": {},
	"Trailer": {}, "Transfer-Encoding": {}, "Upgrade": {},
}

func copyEndToEndHeaders(destination, source http.Header) {
	for key, values := range source {
		canonicalKey := http.CanonicalHeaderKey(key)
		_, hop := hopByHopHeaders[canonicalKey]
		if hop || strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "X-Api-Key") ||
			strings.HasPrefix(strings.ToLower(key), "x-human-") {
			continue
		}
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func validateToolIdentity(config ServerConfig, taskID, idempotencyKey string) error {
	identity := completion.RoutingIdentity{
		CallerID:       strings.TrimSpace(config.CallerID),
		WorkspaceKey:   strings.TrimSpace(config.WorkspaceKey),
		TaskID:         taskID,
		IdempotencyKey: idempotencyKey,
		HarnessID:      adapter.HumanShimID,
		HarnessVersion: adapter.HumanShimVersion,
		Root:           strings.TrimSpace(config.WorkspaceRoot),
	}
	return identity.Validate(completion.TierRemoteTools)
}
