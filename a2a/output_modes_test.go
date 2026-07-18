package a2a

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

func TestAcceptedOutputModesMustCoverAgentCard(t *testing.T) {
	service := openA2ATestAgent(t)
	handler := newRequestHandler(Config{
		Agent: service, Card: testAgentCard(),
		ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			return "workspace-a", nil
		},
	}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
	request := func(id string, modes ...string) *sdk.SendMessageRequest {
		return &sdk.SendMessageRequest{
			Config:  &sdk.SendMessageConfig{ReturnImmediately: true, AcceptedOutputModes: modes},
			Message: &sdk.Message{ID: id, Role: sdk.MessageRoleUser, Parts: sdk.ContentParts{sdk.NewTextPart("hello")}},
		}
	}
	if _, err := handler.acceptMessage(ctx, request("unsupported", "image/png")); !errors.Is(err, sdk.ErrUnsupportedContentType) {
		t.Fatalf("unsupported modes error = %v", err)
	}
	if _, err := handler.acceptMessage(ctx, request("invalid", "not a media type")); !errors.Is(err, sdk.ErrInvalidParams) {
		t.Fatalf("invalid modes error = %v", err)
	}
	if _, err := handler.acceptMessage(ctx, request("partial", "text/plain; charset=utf-8")); !errors.Is(err, sdk.ErrUnsupportedContentType) {
		t.Fatalf("partial modes error = %v", err)
	}
	if _, err := handler.acceptMessage(ctx, request("supported", "image/png", "text/plain; charset=utf-8", "application/json")); err != nil {
		t.Fatalf("supported mode: %v", err)
	}
}

func TestInputPartMustMatchAgentCard(t *testing.T) {
	service := openA2ATestAgent(t)
	handler := newRequestHandler(Config{
		Agent: service, Card: testAgentCard(),
		ResolveWorkspace: func(context.Context, Principal, *sdk.SendMessageRequest) (agent.WorkspaceID, error) {
			return "workspace-a", nil
		},
	}).(*requestHandler)
	ctx := withPrincipal(context.Background(), Principal{Authority: "authority-a", Subject: "caller-a"})
	raw := sdk.NewRawPart([]byte{0, 1, 2})
	raw.MediaType = "image/png"
	_, err := handler.acceptMessage(ctx, &sdk.SendMessageRequest{Message: &sdk.Message{
		ID: "unsupported-input", Role: sdk.MessageRoleUser, Parts: sdk.ContentParts{raw},
	}})
	if !errors.Is(err, sdk.ErrUnsupportedContentType) {
		t.Fatalf("unsupported input error = %v", err)
	}
}
