package llm_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vibe-agi/human/llm"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
)

func newHardeningConfig(t *testing.T, path string) llm.Config {
	t.Helper()
	resource, err := llmsqlite.Open(t.Context(), llmsqlite.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	return llm.Config{
		DeploymentID: "hardening-deployment", Store: resource,
		Codecs: []llm.CodecRegistration{{
			Codec: testCodec{}, StreamContentType: "text/event-stream",
			AggregateContentType: "application/json", SuccessStatus: 200,
		}},
		Clock: &stepClock{next: time.Unix(1_800_000_000, 0)}, IDs: &sequenceIDs{},
	}
}

func startHardeningService(t *testing.T, config llm.Config) *llm.Service {
	t.Helper()
	service, err := llm.NewService(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-service.Done():
		default:
			shutdownRuntime(t, service)
		}
	})
	return service
}

func TestServiceRequiresExplicitAdmissionPolicy(t *testing.T) {
	missing := newHardeningConfig(t, filepath.Join(t.TempDir(), "missing.db"))
	if _, err := llm.NewService(t.Context(), missing); !errors.Is(err, llm.ErrInvalidServiceConfig) {
		t.Fatalf("nil admission policy = %v, want ErrInvalidServiceConfig", err)
	}

	explicit := newHardeningConfig(t, filepath.Join(t.TempDir(), "explicit.db"))
	explicit.Admission = llm.AdmitAll()
	service := startHardeningService(t, explicit)
	worker := openTestWorker(t, service, "worker-a", "session-a")
	result, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "allow-a", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "explicit allow-all")),
	})
	if err != nil {
		t.Fatalf("AdmitAll admission: %v", err)
	}
	assignment := receiveServiceAssignment(t, worker)
	if assignment.Assignment.Identity != result.Identity {
		t.Fatalf("assignment identity = %+v, want %+v", assignment.Assignment.Identity, result.Identity)
	}
}

func TestServiceAdmissionPolicyAndRouterReceiveCallerAttributes(t *testing.T) {
	attributes := map[string]any{"org": "acme", "tier": "gold"}
	var policySeen, routerSeen map[string]any
	config := newHardeningConfig(t, filepath.Join(t.TempDir(), "attributes.db"))
	config.Admission = llm.AdmissionPolicyFunc(func(_ context.Context, input llm.AdmissionContext) (llm.AdmissionPolicyDecision, error) {
		policySeen = input.CallerAttributes
		// A misbehaving policy mutating its borrowed view must not leak into the
		// router's view or the caller's original map.
		delete(input.CallerAttributes, "tier")
		return llm.AdmissionPolicyDecision{Allowed: true}, nil
	})
	config.Router = llm.WorkerRouterFunc(func(_ context.Context, input llm.WorkerRouteRequest) (llm.WorkerID, error) {
		routerSeen = input.CallerAttributes
		return "worker-a", nil
	})
	service := startHardeningService(t, config)
	openTestWorker(t, service, "worker-a", "session-a")
	if _, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "attributes-a", CodecID: testCodecID,
		Body:             mustJSON(t, testRequest(true, "attributes")),
		CallerAttributes: attributes,
	}); err != nil {
		t.Fatal(err)
	}
	if policySeen == nil || policySeen["org"] != "acme" {
		t.Fatalf("admission policy attributes = %#v", policySeen)
	}
	want := map[string]any{"org": "acme", "tier": "gold"}
	if !reflect.DeepEqual(routerSeen, want) {
		t.Fatalf("router attributes = %#v, want %#v", routerSeen, want)
	}
	if !reflect.DeepEqual(attributes, want) {
		t.Fatalf("caller-owned attributes were mutated: %#v", attributes)
	}
}

func TestServiceCallerAttributesStayOutOfRequestIdentity(t *testing.T) {
	config := newHardeningConfig(t, filepath.Join(t.TempDir(), "identity.db"))
	config.Admission = llm.AdmitAll()
	config.Router = llm.WorkerRouterFunc(func(context.Context, llm.WorkerRouteRequest) (llm.WorkerID, error) {
		return "worker-a", nil
	})
	service := startHardeningService(t, config)
	openTestWorker(t, service, "worker-a", "session-a")
	first, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "identity-a", CodecID: testCodecID,
		Body:             mustJSON(t, testRequest(true, "identity")),
		CallerAttributes: map[string]any{"org": "acme"},
	})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "identity-a", CodecID: testCodecID,
		Body:             mustJSON(t, testRequest(true, "identity")),
		CallerAttributes: map[string]any{"org": "other", "extra": true},
	})
	if err != nil || !replay.Replay || replay.Identity != first.Identity ||
		replay.RequestDigest != first.RequestDigest {
		t.Fatalf("attributes changed exact-retry identity: %+v, %v", replay, err)
	}
}

func TestServiceMultipleWorkersWithoutRouterFailClosed(t *testing.T) {
	config := newHardeningConfig(t, filepath.Join(t.TempDir(), "router.db"))
	config.Admission = llm.AdmitAll()
	service := startHardeningService(t, config)

	var failure *llm.AdmissionError
	_, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "router-none", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "no workers")),
	})
	if !errors.As(err, &failure) || failure.Failure.Code != "worker_unavailable" ||
		!errors.Is(err, llm.ErrWorkerUnavailable) {
		t.Fatalf("zero-worker admission = %v", err)
	}

	workerA := openTestWorker(t, service, "worker-a", "session-a")
	single, err := service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "router-one", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "single worker")),
	})
	if err != nil {
		t.Fatalf("single-worker admission: %v", err)
	}
	assignment := receiveServiceAssignment(t, workerA)
	if assignment.Assignment.Identity != single.Identity {
		t.Fatalf("single-worker assignment = %+v, want %+v", assignment.Assignment.Identity, single.Identity)
	}

	openTestWorker(t, service, "worker-b", "session-b")
	_, err = service.Admit(t.Context(), llm.AdmissionRequest{
		CallerID: "caller-a", IdempotencyKey: "router-many", CodecID: testCodecID,
		Body: mustJSON(t, testRequest(true, "ambiguous workers")),
	})
	if !errors.As(err, &failure) {
		t.Fatalf("multi-worker admission without router = %v", err)
	}
	if failure.Failure.Status != 500 || failure.Failure.Code != "worker_router_required" ||
		!errors.Is(err, llm.ErrWorkerRouterRequired) {
		t.Fatalf("multi-worker failure = %+v (cause %v)", failure.Failure, err)
	}
}
