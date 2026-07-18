// Package a2a exposes the HTTP+JSON binding for the durable HumanAgent
// surface. Protocol DTOs and wire framing come from the official A2A Go SDK;
// task identity, state, idempotency, leases, and Artifacts remain owned by
// agent.Agent.
package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	sdka2a "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/vibe-agi/human/agent"
)

const (
	defaultPollInterval      = 250 * time.Millisecond
	defaultMaxRequestBytes   = int64(8 << 20)
	maxPrincipalIdentitySize = 512
)

// Principal is the authenticated identity from which every authority-scoped
// domain key is constructed. Tenant, message metadata, and other request-body
// fields must never be substituted for Authority.
type Principal struct {
	Authority  agent.AuthorityID `json:"authority"`
	Subject    string            `json:"subject"`
	Attributes map[string]any    `json:"attributes,omitempty"`
}

// AuthenticateFunc authenticates one HTTP request before its A2A payload is
// decoded. Implementations may inspect TLS state, headers, host routing, or an
// embedding application's own session context. Non-anonymous requirements must
// be declared consistently in the public Agent Card's security contract.
type AuthenticateFunc func(context.Context, *http.Request) (Principal, error)

// ResolveWorkspaceFunc maps a new A2A task to an authenticated Workspace. It
// is called only for task creation; continuations resolve the already-committed
// task key and cannot move it to a different Workspace.
type ResolveWorkspaceFunc func(context.Context, Principal, *sdka2a.SendMessageRequest) (agent.WorkspaceID, error)

// AuthorizeApplyReceiptFunc authorizes the caller-side CAS journal to report a
// terminal outcome for one published Artifact. Returning an error denies the
// operation.
type AuthorizeApplyReceiptFunc func(context.Context, Principal, agent.Task, *RecordApplyReceiptRequest) error

// Config composes the official A2A HTTP+JSON transport over an existing
// durable Agent. NewHandler does not take ownership of Agent and does not close
// it.
type Config struct {
	// Agent is the durable domain instance served by this handler.
	Agent *agent.Agent
	// Card is snapshotted at construction and served at the standard well-known
	// path. Its interfaces, security, extensions, skills, and MIME modes must
	// describe the mounted handler and its workers truthfully.
	Card *sdka2a.AgentCard
	// Authenticate establishes the Authority/Subject for every non-Card request.
	Authenticate AuthenticateFunc
	// ResolveWorkspace selects the correctness scope for a new Task only.
	ResolveWorkspace ResolveWorkspaceFunc
	// AuthorizeApplyReceipt must be configured if and only if Card advertises
	// ApplyReceiptExtensionURI.
	AuthorizeApplyReceipt AuthorizeApplyReceiptFunc

	// PollInterval controls durable event polling. Zero selects 250ms.
	PollInterval time.Duration
	// KeepAliveInterval enables SDK SSE keepalive comments when positive. Zero
	// leaves keepalives disabled.
	KeepAliveInterval time.Duration
	// MaxRequestBytes bounds buffered request bodies. Zero selects 8 MiB.
	MaxRequestBytes int64
}

type handlerConfig struct {
	Config
	requiredExtensionURIs []string
	supportedExtensions   map[string]sdka2a.AgentExtension
}

// NewHandler constructs a self-contained A2A HTTP handler. The public Agent
// Card is served at the standard well-known path without authentication; every
// protocol operation is protected by Authenticate and the protocol guard.
func NewHandler(config Config) (http.Handler, error) {
	checked, err := checkConfig(config)
	if err != nil {
		return nil, err
	}
	requestHandler := newRequestHandler(checked.Config)
	return newHTTPHandler(checked, requestHandler), nil
}

func newHTTPHandler(config handlerConfig, requestHandler a2asrv.RequestHandler) http.Handler {
	intercepted := &a2asrv.InterceptedHandler{
		Handler: requestHandler,
		Interceptors: []a2asrv.CallInterceptor{principalInterceptor{
			supportedExtensions: cloneExtensions(config.supportedExtensions),
		}},
	}
	transportOptions := make([]a2asrv.TransportOption, 0, 1)
	if config.KeepAliveInterval > 0 {
		transportOptions = append(transportOptions, a2asrv.WithTransportKeepAlive(config.KeepAliveInterval))
	}
	operations := http.NewServeMux()
	restHandler := a2asrv.NewRESTHandler(intercepted, transportOptions...)
	operations.Handle("/", restHandler)
	if config.AuthorizeApplyReceipt != nil {
		operations.Handle("POST /tasks/{idAndAction}", &applyReceiptHandler{
			agent: config.Agent, authorize: config.AuthorizeApplyReceipt, fallback: restHandler,
		})
	}
	protocol := &protocolHandler{
		next:                  operations,
		authenticate:          config.Authenticate,
		maxRequestBytes:       config.MaxRequestBytes,
		requiredExtensionURIs: append([]string(nil), config.requiredExtensionURIs...),
		supportedExtensions:   cloneExtensions(config.supportedExtensions),
	}

	mux := http.NewServeMux()
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(config.Card))
	mux.Handle("/", protocol)
	return mux
}

func checkConfig(config Config) (handlerConfig, error) {
	if config.Agent == nil {
		return handlerConfig{}, errors.New("a2a: Agent is required")
	}
	if config.Card == nil {
		return handlerConfig{}, errors.New("a2a: Agent Card is required")
	}
	if config.Authenticate == nil {
		return handlerConfig{}, errors.New("a2a: Authenticate is required")
	}
	if config.ResolveWorkspace == nil {
		return handlerConfig{}, errors.New("a2a: ResolveWorkspace is required")
	}
	if config.PollInterval < 0 {
		return handlerConfig{}, errors.New("a2a: PollInterval cannot be negative")
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.KeepAliveInterval < 0 {
		return handlerConfig{}, errors.New("a2a: KeepAliveInterval cannot be negative")
	}
	if config.MaxRequestBytes < 0 {
		return handlerConfig{}, errors.New("a2a: MaxRequestBytes cannot be negative")
	}
	if config.MaxRequestBytes == 0 {
		config.MaxRequestBytes = defaultMaxRequestBytes
	}
	card, err := cloneAndValidateCard(config.Card)
	if err != nil {
		return handlerConfig{}, err
	}
	config.Card = card

	supported := make(map[string]sdka2a.AgentExtension, len(config.Card.Capabilities.Extensions))
	required := make([]string, 0, len(config.Card.Capabilities.Extensions))
	for _, extension := range config.Card.Capabilities.Extensions {
		uri := strings.TrimSpace(extension.URI)
		parsed, err := url.ParseRequestURI(uri)
		if uri == "" || uri != extension.URI || err != nil || !parsed.IsAbs() {
			return handlerConfig{}, errors.New("a2a: Agent Card extension URI must be canonical and absolute")
		}
		if _, duplicate := supported[uri]; duplicate {
			return handlerConfig{}, fmt.Errorf("a2a: Agent Card declares extension %q more than once", uri)
		}
		supported[uri] = extension
		if extension.Required {
			required = append(required, uri)
		}
	}
	_, applyReceiptDeclared := supported[ApplyReceiptExtensionURI]
	if applyReceiptDeclared != (config.AuthorizeApplyReceipt != nil) {
		return handlerConfig{}, fmt.Errorf(
			"a2a: Agent Card extension %q and Apply Receipt authorization must be configured together",
			ApplyReceiptExtensionURI,
		)
	}

	return handlerConfig{
		Config:                config,
		requiredExtensionURIs: required,
		supportedExtensions:   supported,
	}, nil
}

func cloneAndValidateCard(source *sdka2a.AgentCard) (*sdka2a.AgentCard, error) {
	encoded, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("a2a: encode Agent Card: %w", err)
	}
	var card sdka2a.AgentCard
	if err := json.Unmarshal(encoded, &card); err != nil {
		return nil, fmt.Errorf("a2a: clone Agent Card: %w", err)
	}
	if strings.TrimSpace(card.Name) == "" || strings.TrimSpace(card.Description) == "" ||
		strings.TrimSpace(card.Version) == "" {
		return nil, errors.New("a2a: Agent Card name, description, and version are required")
	}
	if len(card.DefaultInputModes) == 0 || len(card.DefaultOutputModes) == 0 {
		return nil, errors.New("a2a: Agent Card default input and output modes are required")
	}
	for _, mode := range append(append([]string(nil), card.DefaultInputModes...), card.DefaultOutputModes...) {
		if _, ok := normalizedMediaMode(mode); !ok {
			return nil, fmt.Errorf("a2a: Agent Card media mode %q is invalid or non-canonical", mode)
		}
	}
	if len(card.Skills) == 0 {
		return nil, errors.New("a2a: Agent Card must declare at least one skill")
	}
	seenSkills := make(map[string]struct{}, len(card.Skills))
	for index, skill := range card.Skills {
		if strings.TrimSpace(skill.ID) == "" || strings.TrimSpace(skill.Name) == "" ||
			strings.TrimSpace(skill.Description) == "" || len(skill.Tags) == 0 {
			return nil, fmt.Errorf("a2a: Agent Card skill %d requires id, name, description, and tags", index)
		}
		if skill.ID != strings.TrimSpace(skill.ID) || skill.Name != strings.TrimSpace(skill.Name) ||
			skill.Description != strings.TrimSpace(skill.Description) {
			return nil, fmt.Errorf("a2a: Agent Card skill %q fields must be canonical", skill.ID)
		}
		if _, duplicate := seenSkills[skill.ID]; duplicate {
			return nil, fmt.Errorf("a2a: Agent Card skill id %q is declared more than once", skill.ID)
		}
		seenSkills[skill.ID] = struct{}{}
		if len(skill.InputModes) != 0 || len(skill.OutputModes) != 0 {
			return nil, fmt.Errorf(
				"a2a: Agent Card skill %q declares per-skill MIME overrides, which this handler does not implement",
				skill.ID,
			)
		}
		seenTags := make(map[string]struct{}, len(skill.Tags))
		for _, tag := range skill.Tags {
			if strings.TrimSpace(tag) == "" || tag != strings.TrimSpace(tag) {
				return nil, fmt.Errorf("a2a: Agent Card skill %q has an invalid tag", skill.ID)
			}
			if _, duplicate := seenTags[tag]; duplicate {
				return nil, fmt.Errorf("a2a: Agent Card skill %q repeats tag %q", skill.ID, tag)
			}
			seenTags[tag] = struct{}{}
		}
		for _, mode := range append(append([]string(nil), skill.InputModes...), skill.OutputModes...) {
			if _, ok := normalizedMediaMode(mode); !ok {
				return nil, fmt.Errorf("a2a: Agent Card skill %q media mode %q is invalid or non-canonical", skill.ID, mode)
			}
		}
	}
	if err := validateSecuritySchemes(card.SecuritySchemes); err != nil {
		return nil, err
	}
	if err := validateSecurityRequirements("Agent Card", card.SecurityRequirements, card.SecuritySchemes); err != nil {
		return nil, err
	}
	for _, skill := range card.Skills {
		if err := validateSecurityRequirements(
			fmt.Sprintf("Agent Card skill %q", skill.ID), skill.SecurityRequirements, card.SecuritySchemes,
		); err != nil {
			return nil, err
		}
	}
	if !card.Capabilities.Streaming {
		return nil, errors.New("a2a: Agent Card must declare the streaming capability served by this handler")
	}
	if card.Capabilities.PushNotifications {
		return nil, errors.New("a2a: push notifications are not implemented by this handler")
	}
	if card.Capabilities.ExtendedAgentCard {
		return nil, errors.New("a2a: extended Agent Cards are not implemented by this handler")
	}
	hasHTTPJSON := false
	for _, candidate := range card.SupportedInterfaces {
		if candidate == nil {
			return nil, errors.New("a2a: Agent Card contains a nil interface")
		}
		parsed, err := url.Parse(candidate.URL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("a2a: Agent Card interface URL %q is not absolute", candidate.URL)
		}
		if candidate.ProtocolBinding == sdka2a.TransportProtocolHTTPJSON &&
			candidate.ProtocolVersion == sdka2a.Version {
			if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.Fragment != "" {
				return nil, fmt.Errorf(
					"a2a: Agent Card HTTP+JSON interface URL %q must use http/https without userinfo or fragment",
					candidate.URL,
				)
			}
			if candidate.Tenant != "" {
				return nil, errors.New("a2a: tenant-qualified Agent interfaces are not implemented by this handler")
			}
			hasHTTPJSON = true
		}
	}
	if !hasHTTPJSON {
		return nil, fmt.Errorf(
			"a2a: Agent Card must declare an HTTP+JSON interface for protocol %s",
			sdka2a.Version,
		)
	}
	return &card, nil
}

func validateSecuritySchemes(schemes sdka2a.NamedSecuritySchemes) error {
	for name, scheme := range schemes {
		if !validDurableIdentity(string(name)) {
			return fmt.Errorf("a2a: Agent Card security scheme name %q is invalid", name)
		}
		switch value := scheme.(type) {
		case sdka2a.APIKeySecurityScheme:
			switch value.Location {
			case sdka2a.APIKeySecuritySchemeLocationHeader,
				sdka2a.APIKeySecuritySchemeLocationQuery,
				sdka2a.APIKeySecuritySchemeLocationCookie:
			default:
				return fmt.Errorf("a2a: Agent Card API key scheme %q has an invalid location", name)
			}
			if !validHTTPToken(value.Name) {
				return fmt.Errorf("a2a: Agent Card API key scheme %q has an invalid parameter name", name)
			}
		case sdka2a.HTTPAuthSecurityScheme:
			if !validHTTPToken(value.Scheme) {
				return fmt.Errorf("a2a: Agent Card HTTP auth scheme %q has an invalid scheme", name)
			}
		case sdka2a.OpenIDConnectSecurityScheme:
			if !validHTTPSURL(value.OpenIDConnectURL) {
				return fmt.Errorf("a2a: Agent Card OpenID Connect scheme %q has an invalid discovery URL", name)
			}
		case sdka2a.MutualTLSSecurityScheme:
		case sdka2a.OAuth2SecurityScheme:
			if value.Oauth2MetadataURL != "" && !validHTTPSURL(value.Oauth2MetadataURL) {
				return fmt.Errorf("a2a: Agent Card OAuth2 scheme %q has an invalid metadata URL", name)
			}
		default:
			return fmt.Errorf("a2a: Agent Card security scheme %q has unsupported type %T", name, scheme)
		}
	}
	return nil
}

func validHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for _, current := range []byte(value) {
		if (current >= '0' && current <= '9') || (current >= 'A' && current <= 'Z') ||
			(current >= 'a' && current <= 'z') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(current)) {
			continue
		}
		return false
	}
	return true
}

func validHTTPSURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" &&
		parsed.User == nil && parsed.Fragment == ""
}

func validateSecurityRequirements(
	label string,
	options sdka2a.SecurityRequirementsOptions,
	schemes sdka2a.NamedSecuritySchemes,
) error {
	for optionIndex, requirement := range options {
		for name, scopes := range requirement {
			canonical := strings.TrimSpace(string(name))
			if canonical == "" || canonical != string(name) {
				return fmt.Errorf("a2a: %s security option %d has an invalid scheme name", label, optionIndex)
			}
			if _, declared := schemes[name]; !declared {
				return fmt.Errorf("a2a: %s security option %d references undeclared scheme %q", label, optionIndex, name)
			}
			seenScopes := make(map[string]struct{}, len(scopes))
			for _, scope := range scopes {
				if strings.TrimSpace(scope) == "" || scope != strings.TrimSpace(scope) {
					return fmt.Errorf("a2a: %s security scheme %q has an invalid scope", label, name)
				}
				if _, duplicate := seenScopes[scope]; duplicate {
					return fmt.Errorf("a2a: %s security scheme %q repeats scope %q", label, name, scope)
				}
				seenScopes[scope] = struct{}{}
			}
		}
	}
	return nil
}

func cloneExtensions(source map[string]sdka2a.AgentExtension) map[string]sdka2a.AgentExtension {
	cloned := make(map[string]sdka2a.AgentExtension, len(source))
	for uri, extension := range source {
		cloned[uri] = extension
	}
	return cloned
}
