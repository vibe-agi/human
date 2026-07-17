package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/store"
)

type workerRouterFunc func(context.Context, WorkerRouteRequest) (string, error)

func (function workerRouterFunc) RouteWorker(
	ctx context.Context,
	request WorkerRouteRequest,
) (string, error) {
	return function(ctx, request)
}

type tenantAuthenticator struct{}

func (tenantAuthenticator) AuthenticateRequest(request *http.Request) (auth.Principal, error) {
	secret := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	switch secret {
	case "tenant-a-token":
		return auth.Principal{Type: auth.PrincipalCaller, SubjectID: "tenant-a", KeyID: "key-tenant-a"}, nil
	case "tenant-b-token":
		return auth.Principal{Type: auth.PrincipalCaller, SubjectID: "tenant-b", KeyID: "key-tenant-b"}, nil
	default:
		return auth.Principal{}, auth.ErrUnauthorized
	}
}

func TestWorkerRouterIsolatesTenantsAndReceivesValidatedIdentity(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	routed := make(map[string]WorkerRouteRequest)
	fixture := newGatewayFixtureWithAuthenticator(t, false, Config{
		WorkerRouter: workerRouterFunc(func(ctx context.Context, request WorkerRouteRequest) (string, error) {
			if ctx != request.HTTPRequest.Context() {
				return "", errors.New("router request lost its context")
			}
			body, err := io.ReadAll(request.HTTPRequest.Body)
			if err != nil || !strings.Contains(string(body), "route me") {
				return "", errors.New("router did not receive a readable request body")
			}
			mu.Lock()
			routed[request.Caller.SubjectID] = request
			mu.Unlock()
			return "worker-" + strings.TrimPrefix(request.Caller.SubjectID, "tenant-"), nil
		}),
	}, tenantAuthenticator{})
	if err := fixture.registry.Register(adapter.Profile{
		HarnessID: "known", HarnessVersion: "1", Read: &adapter.Tool{Name: "read_file"}, ErrorShape: "is_error",
	}); err != nil {
		t.Fatal(err)
	}
	workerA, err := fixture.hub.Register("worker-a")
	if err != nil {
		t.Fatal(err)
	}
	defer workerA.Close()
	workerB, err := fixture.hub.Register("worker-b")
	if err != nil {
		t.Fatal(err)
	}
	defer workerB.Close()

	requestAndComplete := func(tenant, token string, target, other *hubWorkerView) {
		t.Helper()
		body := chatBody("route me for "+tenant, true)
		request, err := http.NewRequest(http.MethodPost, fixture.server.URL+"/v1/chat/completions", strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(headerIdempotencyKey, "request-"+tenant)
		request.Header.Set("X-Route-Probe", tenant)
		setRemoteHeaders(request, "task-"+tenant)
		result := make(chan error, 1)
		go func() {
			response, requestErr := http.DefaultClient.Do(request)
			if requestErr != nil {
				result <- requestErr
				return
			}
			responseBody, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil {
				result <- readErr
				return
			}
			if response.StatusCode != http.StatusOK || !strings.Contains(string(responseBody), "routed "+tenant) {
				result <- errors.New("unexpected routed completion response")
				return
			}
			result <- nil
		}()
		var assignment completion.Assignment
		select {
		case assignment = <-target.assignments:
		case <-time.After(time.Second):
			t.Fatal("selected worker did not receive assignment")
		}
		select {
		case leaked := <-other.assignments:
			t.Fatalf("tenant assignment leaked to another worker: %+v", leaked)
		case <-time.After(20 * time.Millisecond):
		}
		if assignment.CallerID != tenant || assignment.LeaseOwner != target.id {
			t.Fatalf("routed assignment = %+v", assignment)
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventAccepted, WorkerID: target.id,
		}); err != nil {
			t.Fatal(err)
		}
		if err := fixture.hub.Publish(context.Background(), assignment.CallerID, assignment.IdempotencyKey, completion.Event{
			Type: completion.EventFinal, Text: "routed " + tenant,
		}); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		task, err := fixture.db.GetTask(context.Background(), store.TaskKey{CallerID: tenant, TaskID: "task-" + tenant})
		if err != nil || task.LeaseOwner != target.id {
			t.Fatalf("durable routed owner = %+v, %v", task, err)
		}
	}

	a := hubWorkerView{id: "worker-a", assignments: workerA.Assignments}
	b := hubWorkerView{id: "worker-b", assignments: workerB.Assignments}
	requestAndComplete("tenant-a", "tenant-a-token", &a, &b)
	requestAndComplete("tenant-b", "tenant-b-token", &b, &a)

	mu.Lock()
	defer mu.Unlock()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		input := routed[tenant]
		if input.Caller.SubjectID != tenant || input.Model != "human-expert" ||
			input.Tier != completion.TierRemoteTools || input.Identity.WorkspaceKey != "workspace" ||
			input.Identity.TaskID != "task-"+tenant || input.Identity.HarnessID != "known" ||
			input.Identity.HarnessVersion != "1" || input.HTTPRequest.Header.Get("X-Route-Probe") != tenant {
			t.Fatalf("router input for %s = %+v", tenant, input)
		}
	}
}

type hubWorkerView struct {
	id          string
	assignments <-chan completion.Assignment
}

func TestWorkerRouterDenialFailureAndUnavailableAreDistinct(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		route      func(context.Context, WorkerRouteRequest) (string, error)
		wantStatus int
		wantCode   string
	}{
		{
			name: "policy denial",
			route: func(context.Context, WorkerRouteRequest) (string, error) {
				return "", ErrWorkerRouteDenied
			},
			wantStatus: http.StatusForbidden, wantCode: "worker_route_denied",
		},
		{
			name: "router failure",
			route: func(context.Context, WorkerRouteRequest) (string, error) {
				return "", errors.New("policy database unavailable")
			},
			wantStatus: http.StatusInternalServerError, wantCode: "worker_route_error",
		},
		{
			name: "selected worker unavailable",
			route: func(context.Context, WorkerRouteRequest) (string, error) {
				return "offline-worker", nil
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "worker_unavailable",
		},
		{
			name: "invalid worker subject",
			route: func(context.Context, WorkerRouteRequest) (string, error) {
				return "not a stable subject", nil
			},
			wantStatus: http.StatusInternalServerError, wantCode: "worker_route_error",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewayFixtureWithConfig(t, true, Config{WorkerRouter: workerRouterFunc(test.route)})
			request := newChatRequest(t, fixture, chatBody("route decision", false), "route-decision")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.wantStatus || !strings.Contains(string(body), test.wantCode) {
				t.Fatalf("route decision = %d, %s", response.StatusCode, body)
			}
			select {
			case assignment := <-fixture.worker.Assignments:
				t.Fatalf("failed route created an assignment: %+v", assignment)
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}
