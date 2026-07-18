package a2a

import (
	"context"
	"testing"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/vibe-agi/human/agent"
)

func TestCheckConfigSnapshotsAgentCard(t *testing.T) {
	config := testHandlerConfig(t, testAuthenticator).Config
	source := config.Card
	checked, err := checkConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	source.Name = "mutated"
	source.DefaultInputModes[0] = "application/octet-stream"
	source.SupportedInterfaces[0].URL = "https://mutated.invalid"
	source.Skills[0].Name = "mutated"
	source.Capabilities.Extensions[0].URI = "https://mutated.invalid/extension"

	if checked.Card.Name != "Human Agent" || checked.Card.DefaultInputModes[0] != "text/plain" ||
		checked.Card.SupportedInterfaces[0].URL != "http://example.test" ||
		checked.Card.Skills[0].Name != "Human collaboration" ||
		checked.Card.Capabilities.Extensions[0].URI != testExtensionURI {
		t.Fatalf("checked Agent Card changed after source mutation: %#v", checked.Card)
	}
}

func TestCheckConfigRejectsInaccurateAgentCard(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*sdk.AgentCard)
	}{
		{name: "streaming omitted", mutate: func(card *sdk.AgentCard) { card.Capabilities.Streaming = false }},
		{name: "push claimed", mutate: func(card *sdk.AgentCard) { card.Capabilities.PushNotifications = true }},
		{name: "extended card claimed", mutate: func(card *sdk.AgentCard) { card.Capabilities.ExtendedAgentCard = true }},
		{name: "skills omitted", mutate: func(card *sdk.AgentCard) { card.Skills = nil }},
		{name: "skill required field omitted", mutate: func(card *sdk.AgentCard) { card.Skills[0].Tags = nil }},
		{name: "duplicate skill id", mutate: func(card *sdk.AgentCard) { card.Skills = append(card.Skills, card.Skills[0]) }},
		{name: "invalid skill mode", mutate: func(card *sdk.AgentCard) { card.Skills[0].OutputModes = []string{"not a media type"} }},
		{name: "unsupported skill mode override", mutate: func(card *sdk.AgentCard) { card.Skills[0].OutputModes = []string{"text/plain"} }},
		{name: "undeclared card security", mutate: func(card *sdk.AgentCard) {
			card.SecurityRequirements = sdk.SecurityRequirementsOptions{{"missing": {}}}
		}},
		{name: "undeclared skill security", mutate: func(card *sdk.AgentCard) {
			card.Skills[0].SecurityRequirements = sdk.SecurityRequirementsOptions{{"missing": {}}}
		}},
		{name: "invalid default mode", mutate: func(card *sdk.AgentCard) { card.DefaultOutputModes[0] = "not a media type" }},
		{name: "relative interface", mutate: func(card *sdk.AgentCard) { card.SupportedInterfaces[0].URL = "/a2a" }},
		{name: "wrong binding", mutate: func(card *sdk.AgentCard) {
			card.SupportedInterfaces[0].ProtocolBinding = sdk.TransportProtocolJSONRPC
		}},
		{name: "wrong protocol", mutate: func(card *sdk.AgentCard) {
			card.SupportedInterfaces[0].ProtocolVersion = "0.3"
		}},
		{name: "tenant-qualified interface", mutate: func(card *sdk.AgentCard) {
			card.SupportedInterfaces[0].Tenant = "tenant-a"
		}},
		{name: "ftp HTTP JSON interface", mutate: func(card *sdk.AgentCard) {
			card.SupportedInterfaces[0].URL = "ftp://example.test/a2a"
		}},
		{name: "userinfo HTTP JSON interface", mutate: func(card *sdk.AgentCard) {
			card.SupportedInterfaces[0].URL = "https://user@example.test/a2a"
		}},
		{name: "fragment HTTP JSON interface", mutate: func(card *sdk.AgentCard) {
			card.SupportedInterfaces[0].URL = "https://example.test/a2a#fragment"
		}},
		{name: "relative extension URI", mutate: func(card *sdk.AgentCard) {
			card.Capabilities.Extensions[0].URI = "relative-extension"
		}},
		{name: "invalid API key security scheme", mutate: func(card *sdk.AgentCard) {
			card.SecuritySchemes = sdk.NamedSecuritySchemes{"api": sdk.APIKeySecurityScheme{Name: "bad name", Location: "header"}}
		}},
		{name: "invalid HTTP auth security scheme", mutate: func(card *sdk.AgentCard) {
			card.SecuritySchemes = sdk.NamedSecuritySchemes{"http": sdk.HTTPAuthSecurityScheme{Scheme: "bad scheme"}}
		}},
		{name: "insecure OpenID Connect URL", mutate: func(card *sdk.AgentCard) {
			card.SecuritySchemes = sdk.NamedSecuritySchemes{"oidc": sdk.OpenIDConnectSecurityScheme{OpenIDConnectURL: "http://example.test/.well-known/openid-configuration"}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testHandlerConfig(t, testAuthenticator).Config
			test.mutate(config.Card)
			if _, err := checkConfig(config); err == nil {
				t.Fatal("checkConfig accepted an inaccurate Agent Card")
			}
		})
	}
}

func TestCheckConfigRequiresApplyReceiptCardAndAuthorizerTogether(t *testing.T) {
	config := testHandlerConfig(t, testAuthenticator).Config
	config.Card.Capabilities.Extensions = append(config.Card.Capabilities.Extensions, sdk.AgentExtension{
		URI: ApplyReceiptExtensionURI,
	})
	if _, err := checkConfig(config); err == nil {
		t.Fatal("checkConfig accepted an advertised Apply Receipt endpoint without an authorizer")
	}

	config.Card.Capabilities.Extensions = config.Card.Capabilities.Extensions[:1]
	config.AuthorizeApplyReceipt = func(context.Context, Principal, agent.Task, *RecordApplyReceiptRequest) error {
		return nil
	}
	if _, err := checkConfig(config); err == nil {
		t.Fatal("checkConfig accepted an Apply Receipt authorizer without an advertised endpoint")
	}
}
