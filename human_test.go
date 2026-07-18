package human_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"

	human "github.com/vibe-agi/human"
	"github.com/vibe-agi/human/gateway"
)

func TestLLMFacadeKeepsGatewayTypeAndDefaults(t *testing.T) {
	var _ http.Handler = (*human.LLM)(nil)
	var rootConfig human.LLMConfig = gateway.DefaultConfig()
	var gatewayConfig gateway.Config = human.DefaultLLMConfig()
	if !reflect.DeepEqual(rootConfig, gatewayConfig) {
		t.Fatalf("root defaults differ from gateway defaults:\nroot = %#v\ngateway = %#v", rootConfig, gatewayConfig)
	}
	if human.LLMWorkerPath != gateway.WorkerPath {
		t.Fatalf("worker path = %q, want %q", human.LLMWorkerPath, gateway.WorkerPath)
	}

	// These assignments are compile-time compatibility assertions. The facade
	// must not grow a wrapper type with a second lifecycle or method set.
	var rootServer *human.LLM
	var gatewayServer *gateway.Server = rootServer
	rootServer = gatewayServer
	_ = rootServer
}

func TestNewLLMAndGatewayCrossRecoverSameSchema(t *testing.T) {
	ctx := context.Background()
	config := human.DefaultLLMConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	root, err := human.NewLLM(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	direct, err := gateway.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := human.NewLLM(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}

func TestNewLLMOpensRealModelHandler(t *testing.T) {
	config := human.DefaultLLMConfig()
	config.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")

	server, err := human.NewLLM(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close HumanLLM: %v", err)
		}
	})

	issued, err := server.Issue(context.Background(), gateway.PrincipalCaller, "facade-caller")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer "+issued.Secret)
	response := httptest.NewRecorder()
	server.ModelHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("models status = %d, body = %s", response.Code, response.Body.String())
	}
}
