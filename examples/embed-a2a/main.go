// Command embed-a2a demonstrates mounting HumanAgent's A2A 1.0 HTTP+JSON
// caller surface inside an application-owned router.
//
// The X-Example-* headers below are deliberately NOT a production
// authentication scheme. They only make the host callback boundary visible.
// Production code must either verify JWT/session/mTLS inside Authenticate, or
// trust identity headers only behind a verified proxy hop that strips client
// copies and prevents direct access to this handler.
//
// This example intentionally does not listen on a socket and does not create a
// fake worker. A real host owns the HTTP server and connects a separately
// authenticated HumanAgent worker transport.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	sdka2a "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	human "github.com/vibe-agi/human"
	"github.com/vibe-agi/human/a2a"
	"github.com/vibe-agi/human/agent"
	agentsqlite "github.com/vibe-agi/human/agent/sqlite"
)

const (
	headerAuthenticated = "X-Example-Authenticated"
	headerAuthority     = "X-Example-Authority"
	headerSubject       = "X-Example-Subject"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (resultErr error) {
	databasePath := os.Getenv("HUMAN_EMBED_A2A_DB")
	if databasePath == "" {
		userConfig, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		databasePath = filepath.Join(userConfig, "human", "examples", "a2a-agent.db")
	}

	ctx := context.Background()
	store, err := agentsqlite.Open(ctx, agentsqlite.Config{Path: databasePath})
	if err != nil {
		return fmt.Errorf("open embedded HumanAgent Store: %w", err)
	}
	config := human.DefaultAgentConfig()
	config.Store = store
	service, err := human.NewAgent(ctx, config)
	if err != nil {
		return fmt.Errorf("open embedded HumanAgent: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, service.Close()) }()

	handler, err := a2a.NewHandler(a2a.Config{
		Agent:            service,
		Card:             exampleAgentCard(),
		Authenticate:     authenticateExampleRequest,
		ResolveWorkspace: resolveExampleWorkspace,
		AuthorizeApplyReceipt: func(
			_ context.Context,
			principal a2a.Principal,
			task agent.Task,
			_ *a2a.RecordApplyReceiptRequest,
		) error {
			if principal.Subject != "caller-a" || task.Ref.Workspace.Authority != principal.Authority {
				return sdka2a.ErrUnauthorized
			}
			return nil
		},
	})
	if err != nil {
		return fmt.Errorf("construct A2A handler: %w", err)
	}

	applicationMux := http.NewServeMux()
	// Agent Card discovery stays at the origin-wide standard well-known path;
	// only protocol operations are mounted below the application's prefix.
	applicationMux.Handle(a2asrv.WellKnownAgentCardPath, handler)
	applicationMux.Handle("/human-agent/", http.StripPrefix("/human-agent", handler))

	// Pass applicationMux to the application-owned HTTP server. Keeping it local
	// here makes it impossible for the example to accidentally open a listener.
	fmt.Println("HumanAgent A2A operations mounted at /human-agent/ and Agent Card at /.well-known/agent-card.json (not listening; demo headers are not production auth)")
	return nil
}

func authenticateExampleRequest(_ context.Context, request *http.Request) (a2a.Principal, error) {
	// A marker header is not authentication. This callback is intentionally
	// simple so the embedding point is obvious; see the package comment above.
	if request.Header.Get(headerAuthenticated) != "1" {
		return a2a.Principal{}, sdka2a.ErrUnauthenticated
	}
	authority := agent.AuthorityID(strings.TrimSpace(request.Header.Get(headerAuthority)))
	subject := strings.TrimSpace(request.Header.Get(headerSubject))
	if authority != "tenant-a" || subject != "caller-a" {
		return a2a.Principal{}, sdka2a.ErrUnauthenticated
	}
	return a2a.Principal{Authority: authority, Subject: subject}, nil
}

func resolveExampleWorkspace(
	_ context.Context,
	principal a2a.Principal,
	_ *sdka2a.SendMessageRequest,
) (agent.WorkspaceID, error) {
	// Workspace routing is host policy. Request body tenant/metadata is never
	// allowed to replace the authenticated Authority.
	if principal.Authority != "tenant-a" || principal.Subject != "caller-a" {
		return "", sdka2a.ErrUnauthorized
	}
	return "workspace-a", nil
}

func exampleAgentCard() *sdka2a.AgentCard {
	return &sdka2a.AgentCard{
		Name:        "Embedded Human Agent",
		Description: "Durable tasks delegated to a human collaborator.",
		Version:     "example",
		SupportedInterfaces: []*sdka2a.AgentInterface{
			sdka2a.NewAgentInterface(
				// Replace this with the externally reachable operations URL.
				"https://human.example.invalid/human-agent/",
				sdka2a.TransportProtocolHTTPJSON,
			),
		},
		DefaultInputModes: []string{"text/plain", "application/json"},
		// This must cover every MIME type a real worker may publish as an Artifact.
		DefaultOutputModes: []string{"text/plain", "application/json", "application/octet-stream"},
		SecuritySchemes: sdka2a.NamedSecuritySchemes{
			"exampleAuthenticated": sdka2a.APIKeySecurityScheme{
				Location: sdka2a.APIKeySecuritySchemeLocationHeader, Name: headerAuthenticated,
			},
			"exampleAuthority": sdka2a.APIKeySecurityScheme{
				Location: sdka2a.APIKeySecuritySchemeLocationHeader, Name: headerAuthority,
			},
			"exampleSubject": sdka2a.APIKeySecurityScheme{
				Location: sdka2a.APIKeySecuritySchemeLocationHeader, Name: headerSubject,
			},
		},
		SecurityRequirements: sdka2a.SecurityRequirementsOptions{{
			"exampleAuthenticated": {},
			"exampleAuthority":     {},
			"exampleSubject":       {},
		}},
		Capabilities: sdka2a.AgentCapabilities{
			Streaming: true,
			Extensions: []sdka2a.AgentExtension{
				{
					URI:         a2a.WorkspaceExtensionURI,
					Description: "Carries immutable Workspace/Artifact CAS identities.",
				},
				{
					URI:         a2a.ApplyReceiptExtensionURI,
					Description: "Records an authorized caller-side terminal CAS outcome.",
				},
			},
		},
		Skills: []sdka2a.AgentSkill{{
			ID:          "human-collaboration",
			Name:        "Human collaboration",
			Description: "Delegate a durable, multi-turn task to a human collaborator.",
			Tags:        []string{"human", "workspace", "durable-task"},
		}},
	}
}
