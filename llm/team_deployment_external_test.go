package llm_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/builtin"
	"github.com/vibe-agi/human/llm/callerhttp"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	"github.com/vibe-agi/human/llm/workerws"
	workersqlite "github.com/vibe-agi/human/llm/workerws/sqlite"
)

// TestPublicStackRoutesAuthenticatedTenantsAcrossRemoteWorkers is the minimum
// team-deployment composition on public ports only: the host owns TLS, caller
// authentication owns tenant claims, policy and routing consume those claims,
// and two durable workerws clients receive only their tenant's assignments. No
// request header or body may self-select a tenant or worker.
func TestPublicStackRoutesAuthenticatedTenantsAcrossRemoteWorkers(t *testing.T) {
	store, err := llmsqlite.Open(t.Context(), llmsqlite.Config{
		Path: filepath.Join(t.TempDir(), "team-service.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := llm.NewService(t.Context(), llm.Config{
		DeploymentID: "team-deployment", Store: store, Codecs: builtin.Registrations(),
		Admission: llm.AdmissionPolicyFunc(func(_ context.Context, input llm.AdmissionContext) (llm.AdmissionPolicyDecision, error) {
			tenant, _ := input.CallerAttributes["tenant"].(string)
			role, _ := input.CallerAttributes["role"].(string)
			want := strings.TrimPrefix(string(input.CallerID), "caller-")
			return llm.AdmissionPolicyDecision{Allowed: tenant != "" && tenant == want && role == "member"}, nil
		}),
		Router: llm.WorkerRouterFunc(func(_ context.Context, input llm.WorkerRouteRequest) (llm.WorkerID, error) {
			tenant, _ := input.CallerAttributes["tenant"].(string)
			if tenant != "a" && tenant != "b" {
				return "", fmt.Errorf("unknown tenant")
			}
			return llm.WorkerID("worker-" + tenant), nil
		}),
		ToolAuthorizer: llm.ToolAuthorizerFunc(func(context.Context, llm.ToolAuthorization) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownTeamRuntime(t, service) })

	workerTransport, err := workerws.New(workerws.Config{
		GatewayID: "team-gateway",
		Authenticator: workerws.AuthenticateFunc(func(_ context.Context, request *http.Request) (workerws.Identity, error) {
			switch request.Header.Get("Authorization") {
			case "Bearer worker-token-a":
				return workerws.Identity{Worker: "worker-a"}, nil
			case "Bearer worker-token-b":
				return workerws.Identity{Worker: "worker-b"}, nil
			default:
				return workerws.Identity{}, framework.NewFault(
					framework.CodeUnauthenticated, framework.RetryNever, "invalid worker credential", nil,
				)
			}
		}),
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	workerRuntime, err := workerTransport.Start(t.Context(), service)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownTeamRuntime(t, workerRuntime) })
	workerServer := httptest.NewTLSServer(workerTransport)
	t.Cleanup(workerServer.Close)

	clients := map[string]*workerws.Client{}
	for _, tenant := range []string{"a", "b"} {
		journal, openErr := workersqlite.Open(t.Context(), workersqlite.Config{
			Path: filepath.Join(t.TempDir(), "worker-"+tenant+".db"),
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		header := http.Header{}
		header.Set("Authorization", "Bearer worker-token-"+tenant)
		client, clientErr := workerws.NewClient(t.Context(), workerws.ClientConfig{
			URL:     "wss" + strings.TrimPrefix(workerServer.URL, "https"),
			Gateway: "team-gateway", Worker: llm.WorkerID("worker-" + tenant), Journal: journal,
			HTTPHeader: header, HTTPClient: workerServer.Client(), ConnectTimeout: time.Second, WriteTimeout: time.Second,
			ReconnectMinDelay: time.Millisecond, ReconnectMaxDelay: 10 * time.Millisecond,
			ReconnectResetAfter: time.Second,
		})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		clients[tenant] = client
		deferredClient := client
		t.Cleanup(func() { shutdownTeamRuntime(t, deferredClient) })
	}
	waitForTeamWorkers(t, service, 2)

	callerTransport, err := callerhttp.New(callerhttp.Config{
		Authenticator: callerhttp.AuthenticateFunc(func(_ context.Context, request *http.Request) (callerhttp.Identity, error) {
			switch request.Header.Get("Authorization") {
			case "Bearer caller-token-a":
				return callerhttp.Identity{CallerID: "caller-a", Attributes: map[string]any{"tenant": "a", "role": "member"}}, nil
			case "Bearer caller-token-b":
				return callerhttp.Identity{CallerID: "caller-b", Attributes: map[string]any{"tenant": "b", "role": "member"}}, nil
			case "Bearer caller-token-idp-outage":
				return callerhttp.Identity{}, framework.NewFault(
					framework.CodeUnavailable, framework.RetryBackoff, "identity provider unavailable", nil,
				)
			default:
				return callerhttp.Identity{}, framework.NewFault(
					framework.CodeUnauthenticated, framework.RetryNever, "invalid caller credential", nil,
				)
			}
		}),
		Routes: callerhttp.BuiltinRoutes(), WriteTimeout: time.Second, HeartbeatInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	callerRuntime, err := callerTransport.Start(t.Context(), service)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownTeamRuntime(t, callerRuntime) })
	callerServer := httptest.NewTLSServer(callerTransport)
	t.Cleanup(callerServer.Close)

	for _, tenant := range []string{"a", "b"} {
		t.Run("tenant-"+tenant, func(t *testing.T) {
			final := "final-from-worker-" + tenant
			response := make(chan teamCallerResult, 1)
			go func() {
				body := []byte(`{"model":"human-expert","stream":false,"messages":[{"role":"user","content":"route me"}]}`)
				request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodPost,
					callerServer.URL+"/v1/chat/completions", bytes.NewReader(body))
				if requestErr == nil {
					request.Header.Set("Authorization", "Bearer caller-token-"+tenant)
					request.Header.Set("Content-Type", "application/json")
					request.Header.Set(callerhttp.HeaderIdempotencyKey, "tenant-"+tenant+"-request")
					// This is deliberately hostile input. Routing must use only the
					// authenticator's attributes, never a caller-provided tenant hint.
					request.Header.Set("X-Tenant", map[string]string{"a": "b", "b": "a"}[tenant])
				}
				result := teamCallerResult{err: requestErr}
				if requestErr == nil {
					result.response, result.err = callerServer.Client().Do(request)
				}
				response <- result
			}()

			assignment := receiveTeamAssignment(t, clients[tenant].Assignments())
			other := map[string]string{"a": "b", "b": "a"}[tenant]
			select {
			case leaked := <-clients[other].Assignments():
				t.Fatalf("tenant %s assignment leaked to worker %s: %+v", tenant, other, leaked)
			case <-time.After(100 * time.Millisecond):
			}
			if assignment.Assignment.Lease.Owner != llm.WorkerID("worker-"+tenant) ||
				assignment.Assignment.Identity.CallerID != llm.CallerID("caller-"+tenant) {
				t.Fatalf("tenant %s assignment identity = %+v", tenant, assignment.Assignment)
			}
			if err := clients[tenant].SendEvent(t.Context(), llm.WorkerEventDelivery{
				ID: "delivery-" + llm.WorkerDeliveryID(tenant), Identity: assignment.Assignment.Identity,
				LeaseID: assignment.Assignment.Lease.ID,
				Event:   llm.Event{ID: "event-" + tenant, Type: llm.EventFinal, Text: final},
			}); err != nil {
				t.Fatal(err)
			}
			if err := clients[tenant].ConfirmAssignment(t.Context(), assignment.ID); err != nil {
				t.Fatal(err)
			}

			result := <-response
			if result.err != nil {
				t.Fatal(result.err)
			}
			defer result.response.Body.Close()
			payload, err := io.ReadAll(result.response.Body)
			if err != nil || result.response.StatusCode != http.StatusOK || !bytes.Contains(payload, []byte(final)) {
				t.Fatalf("tenant %s HTTP response = %d %q, error %v",
					tenant, result.response.StatusCode, payload, err)
			}
		})
	}

	for _, rejected := range []struct {
		name, token string
		want        int
	}{
		{name: "invalid-sso-assertion", token: "invalid", want: http.StatusUnauthorized},
		{name: "identity-provider-outage", token: "caller-token-idp-outage", want: http.StatusServiceUnavailable},
	} {
		t.Run(rejected.name, func(t *testing.T) {
			body := []byte(`{"model":"human-expert","stream":false,"messages":[{"role":"user","content":"do not admit"}]}`)
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				callerServer.URL+"/v1/chat/completions", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+rejected.token)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(callerhttp.HeaderIdempotencyKey, "rejected-"+rejected.name)
			response, err := callerServer.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != rejected.want {
				t.Fatalf("status = %d, want %d", response.StatusCode, rejected.want)
			}
			for tenant, client := range clients {
				select {
				case leaked := <-client.Assignments():
					t.Fatalf("rejected identity reached worker %s: %+v", tenant, leaked)
				case <-time.After(50 * time.Millisecond):
				}
			}
		})
	}
}

type teamCallerResult struct {
	response *http.Response
	err      error
}

func waitForTeamWorkers(t *testing.T, service *llm.Service, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := service.Status(t.Context())
		if err == nil && status.WorkersOnline == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := service.Status(t.Context())
	t.Fatalf("team workers online = %d, error %v; want %d", status.WorkersOnline, err, want)
}

func receiveTeamAssignment(t *testing.T, assignments <-chan llm.WorkerAssignmentDelivery) llm.WorkerAssignmentDelivery {
	t.Helper()
	select {
	case assignment := <-assignments:
		return assignment
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for routed remote assignment")
		return llm.WorkerAssignmentDelivery{}
	}
}

func shutdownTeamRuntime(t *testing.T, runtime interface{ Shutdown(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Errorf("shutdown %T: %v", runtime, err)
	}
}
