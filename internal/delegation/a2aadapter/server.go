package a2aadapter

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/vibe-agi/human/internal/delegation"
)

const (
	// DefaultJSONRPCPath is the default official A2A JSON-RPC endpoint.
	DefaultJSONRPCPath = "/a2a/jsonrpc"
	// DefaultHTTPJSONPath is the prefix for official A2A HTTP+JSON routes.
	DefaultHTTPJSONPath = "/a2a/http"
	// BearerSchemeName is the Agent Card name of the HTTP Bearer scheme.
	BearerSchemeName = a2a.SecuritySchemeName("bearer")
)

// ServerConfig configures public discovery URLs, authenticated transports, and
// the single delegation authority projected by Server.
type ServerConfig struct {
	Authority     Authority
	Authenticator BearerAuthenticator
	BaseURL       string
	JSONRPCPath   string
	HTTPJSONPath  string
	Name          string
	Description   string
	Version       string
	RemoteExec    bool
}

// Server exposes one delegation authority using the official A2A JSON-RPC and
// HTTP+JSON transports. The public Agent Card is intentionally unauthenticated;
// every protocol operation requires a Bearer token.
type Server struct {
	card           *a2a.AgentCard
	requestHandler *requestHandler
	handler        http.Handler
}

// NewServer builds a public Agent Card and authenticated JSON-RPC/HTTP+JSON
// handlers without starting a listener.
func NewServer(config ServerConfig) (*Server, error) {
	if config.Authority == nil {
		return nil, errors.New("a2a adapter authority is required")
	}
	if config.Authenticator == nil {
		return nil, errors.New("a2a adapter bearer authenticator is required")
	}
	baseURL, err := parseBaseURL(config.BaseURL)
	if err != nil {
		return nil, err
	}
	jsonRPCPath, err := endpointPath(config.JSONRPCPath, DefaultJSONRPCPath)
	if err != nil {
		return nil, fmt.Errorf("JSON-RPC path: %w", err)
	}
	httpJSONPath, err := endpointPath(config.HTTPJSONPath, DefaultHTTPJSONPath)
	if err != nil {
		return nil, fmt.Errorf("HTTP+JSON path: %w", err)
	}
	if jsonRPCPath == httpJSONPath || strings.HasPrefix(jsonRPCPath+"/", httpJSONPath+"/") ||
		strings.HasPrefix(httpJSONPath+"/", jsonRPCPath+"/") {
		return nil, errors.New("A2A transport paths must not overlap")
	}
	if jsonRPCPath == a2asrv.WellKnownAgentCardPath || httpJSONPath == a2asrv.WellKnownAgentCardPath {
		return nil, errors.New("A2A transport path must not replace the public Agent Card path")
	}

	name := strings.TrimSpace(config.Name)
	if name == "" {
		name = "Human Delegation Agent"
	}
	description := strings.TrimSpace(config.Description)
	if description == "" {
		description = "Delegates asynchronous work to a human and exposes durable task recovery"
	}
	version := strings.TrimSpace(config.Version)
	if version == "" {
		version = "0.1.0"
	}
	extensionParams := map[string]any{
		"requestMetadataKey": RequestMetadataKey,
		"artifactView":       "latest-cumulative-replace",
		"artifactMediaType":  delegation.GitPatchMediaType,
	}
	if config.RemoteExec {
		extensionParams["remoteExec"] = "explicit-caller-approval"
	}
	card := &a2a.AgentCard{
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(endpointURL(baseURL, jsonRPCPath), a2a.TransportProtocolJSONRPC),
			a2a.NewAgentInterface(endpointURL(baseURL, httpJSONPath), a2a.TransportProtocolHTTPJSON),
		},
		Capabilities: a2a.AgentCapabilities{Extensions: []a2a.AgentExtension{{
			URI:         MetadataKey,
			Description: "Human delegation authority metadata and cumulative artifact projection",
			Params:      extensionParams,
		}}},
		DefaultInputModes: []string{"text/plain", "application/json", "application/octet-stream"},
		DefaultOutputModes: []string{
			delegation.GitPatchMediaType,
		},
		Name:        name,
		Description: description,
		Version:     version,
		SecuritySchemes: a2a.NamedSecuritySchemes{
			BearerSchemeName: a2a.HTTPAuthSecurityScheme{
				Scheme: "Bearer", Description: "Bearer token resolving to a stable caller identity",
			},
		},
		SecurityRequirements: a2a.SecurityRequirementsOptions{
			{BearerSchemeName: a2a.SecuritySchemeScopes{}},
		},
		Skills: []a2a.AgentSkill{{
			ID:          "human-delegation",
			Name:        "Asynchronous human delegation",
			Description: "Submit, inspect, continue, list, and cancel durable human tasks",
			Tags:        []string{"human", "delegation", "asynchronous"},
			OutputModes: []string{delegation.GitPatchMediaType},
		}},
	}

	requestHandler := &requestHandler{authority: config.Authority, remoteExec: config.RemoteExec}
	mux := http.NewServeMux()
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	mux.Handle(jsonRPCPath, bearerMiddleware(config.Authenticator, a2asrv.NewJSONRPCHandler(requestHandler)))
	mux.Handle(httpJSONPath+"/", bearerMiddleware(config.Authenticator,
		http.StripPrefix(httpJSONPath, a2asrv.NewRESTHandler(requestHandler))))
	return &Server{card: card, requestHandler: requestHandler, handler: mux}, nil
}

// Handler returns the complete HTTP handler, including the public Agent Card.
func (server *Server) Handler() http.Handler { return server.handler }

// AgentCard returns the immutable card served by Handler. Callers must not
// mutate the returned value after the server begins serving.
func (server *Server) AgentCard() *a2a.AgentCard { return server.card }

// RequestHandler exposes the protocol implementation for advanced composition.
// Direct calls remain unauthenticated because caller identity is installed by
// Server's Bearer middleware.
func (server *Server) RequestHandler() a2asrv.RequestHandler { return server.requestHandler }

func parseBaseURL(raw string) (*url.URL, error) {
	baseURL, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("A2A base URL must be an absolute HTTP(S) URL")
	}
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return nil, fmt.Errorf("A2A base URL scheme must be HTTP or HTTPS")
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("A2A base URL must not contain a query or fragment")
	}
	baseURL.Path = strings.TrimSuffix(baseURL.Path, "/")
	baseURL.RawPath = ""
	return baseURL, nil
}

func endpointPath(value string, fallback string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	if !strings.HasPrefix(value, "/") || value == "/" || path.Clean(value) != value {
		return "", errors.New("must be a clean absolute non-root path without a trailing slash")
	}
	return value, nil
}

func endpointURL(baseURL *url.URL, endpointPath string) string {
	result := *baseURL
	result.Path = strings.TrimSuffix(baseURL.Path, "/") + endpointPath
	return result.String()
}
