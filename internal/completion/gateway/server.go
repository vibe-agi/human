// Package gateway implements the synchronous, model-compatible completion gateway.
package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/adapter"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/completion/dialect"
	"github.com/vibe-agi/human/internal/completion/hub"
	"github.com/vibe-agi/human/internal/ratelimit"
	storeapi "github.com/vibe-agi/human/internal/store"
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
	responseEventStream  = "stream"
	responseEventWire    = "wire"
	responseEventStep    = "step"
	responseEventApplied = "applied"
)

type streamMetadata struct {
	ResponseID     string                   `json:"response_id"`
	Model          string                   `json:"model"`
	CreatedAtUnix  int64                    `json:"created_at_unix"`
	ToolCallPolicy canonical.ToolCallPolicy `json:"tool_call_policy,omitempty"`
}

type persistedStep struct {
	Event         completion.Event `json:"event"`
	Wire          []byte           `json:"wire,omitempty"`
	Done          bool             `json:"done"`
	EncodedAtUnix int64            `json:"encoded_at_unix"`
}

type Config struct {
	Models                      []string
	MaxBodyBytes                int64
	MaxWorkerMessageBytes       int64
	HeartbeatInterval           time.Duration
	StreamWriteTimeout          time.Duration
	MaxPending                  time.Duration
	RateLimit                   ratelimit.Config   // zero values select the limiter defaults
	RateLimiter                 *ratelimit.Limiter // when set, overrides RateLimit
	AuditPayload                bool
	AuditPayloadTTL             time.Duration
	DisableCodexAutoIdempotency bool // emergency kill switch for recognized Codex turn keys
	WorkerRouter                WorkerRouter
	Logger                      *slog.Logger
}

func (config Config) withDefaults() Config {
	if len(config.Models) == 0 {
		config.Models = []string{"human-expert"}
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = workerproto.MaxWireMessageBytes
	}
	if config.MaxWorkerMessageBytes <= 0 {
		config.MaxWorkerMessageBytes = workerproto.MaxWireMessageBytes
	}
	if config.MaxBodyBytes > config.MaxWorkerMessageBytes {
		config.MaxBodyBytes = config.MaxWorkerMessageBytes
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	if config.StreamWriteTimeout <= 0 {
		config.StreamWriteTimeout = 10 * time.Second
	}
	if config.MaxPending <= 0 {
		config.MaxPending = 10 * time.Minute
	}
	if config.AuditPayloadTTL <= 0 {
		config.AuditPayloadTTL = 7 * 24 * time.Hour
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return config
}

type Server struct {
	config           Config
	store            storeapi.CompletionStore
	auth             auth.Authenticator
	hub              *hub.Hub
	adapters         *adapter.Registry
	codecs           map[string]dialect.Codec
	admissionLimiter *ratelimit.Limiter
	audit            storeapi.AuditStore
	handler          http.Handler
	runMu            sync.RWMutex
	runContext       context.Context
	runWait          sync.WaitGroup
	responses        responseNotifier
	logger           *slog.Logger
}

type auditRun struct {
	id         string
	admittedAt time.Time
	acceptedAt time.Time
	completed  bool
}

func NewServer(
	config Config,
	store storeapi.CompletionStore,
	authenticator auth.Authenticator,
	workerHub *hub.Hub,
	adapters *adapter.Registry,
	codecs map[string]dialect.Codec,
) (*Server, error) {
	if store == nil || authenticator == nil || workerHub == nil {
		return nil, errors.New("store, authenticator, and worker hub are required")
	}
	if adapters == nil {
		adapters = adapter.NewRegistry()
	}
	if len(codecs) == 0 {
		return nil, errors.New("at least one completion codec is required")
	}
	config = config.withDefaults()
	admissionLimiter := config.RateLimiter
	if admissionLimiter == nil {
		var err error
		admissionLimiter, err = ratelimit.New(config.RateLimit, nil)
		if err != nil {
			return nil, fmt.Errorf("configure admission rate limit: %w", err)
		}
	}
	server := &Server{
		config: config, store: store, auth: authenticator,
		hub: workerHub, adapters: adapters, codecs: codecs,
		logger:           config.Logger,
		admissionLimiter: admissionLimiter, runContext: context.Background(),
	}
	server.audit, _ = store.(storeapi.AuditStore)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /v1/models", server.models)
	for route := range codecs {
		mux.HandleFunc("POST "+route, server.completion(route))
	}
	server.handler = mux
	return server, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	server.handler.ServeHTTP(response, request)
}

func (server *Server) beginAudit(
	ctx context.Context,
	principal auth.Principal,
	task storeapi.Task,
	requestKey storeapi.RequestKey,
	requestPayload []byte,
) *auditRun {
	if server.audit == nil {
		return nil
	}
	now := time.Now().UTC()
	sum := sha256.Sum256([]byte(requestKey.CallerID + "\x00" + requestKey.IdempotencyKey))
	id := "audit_" + hex.EncodeToString(sum[:])
	if _, err := server.audit.CreateAuditMetadata(ctx, storeapi.AuditMetadata{
		ID: id, CallerID: task.CallerID, WorkspaceKey: task.WorkspaceKey,
		TaskID: task.TaskID, Dialect: task.Dialect, KeyID: principal.KeyID, CreatedAt: now,
	}); err != nil {
		server.logger.Error("create audit metadata", "audit_id", id, "error", err)
		return nil
	}
	if server.config.AuditPayload {
		if _, err := server.audit.StoreAuditPayload(ctx, storeapi.AuditPayload{
			AuditID: id, Kind: "request", Data: bytes.Clone(requestPayload),
			CreatedAt: now, ExpiresAt: now.Add(server.config.AuditPayloadTTL),
		}); err != nil {
			server.logger.Error("store request audit payload", "audit_id", id, "error", err)
		}
	}
	return &auditRun{id: id, admittedAt: now}
}

func (server *Server) completeAudit(
	ctx context.Context,
	audit *auditRun,
	requestKey storeapi.RequestKey,
	errorCode string,
) {
	if audit == nil || audit.completed || server.audit == nil {
		return
	}
	audit.completed = true
	completedAt := time.Now().UTC()
	pendingEnd := completedAt
	var generation time.Duration
	if !audit.acceptedAt.IsZero() {
		pendingEnd = audit.acceptedAt
		generation = completedAt.Sub(audit.acceptedAt)
	}
	pending := pendingEnd.Sub(audit.admittedAt)
	if pending < 0 {
		pending = 0
	}
	if generation < 0 {
		generation = 0
	}
	if _, err := server.audit.CompleteAuditMetadata(
		ctx, audit.id, pending.Milliseconds(), generation.Milliseconds(), errorCode,
	); err != nil {
		server.logger.Error("complete audit metadata", "audit_id", audit.id, "error", err)
	}
	if !server.config.AuditPayload {
		return
	}
	events, err := server.store.ListResponseEvents(ctx, requestKey, 0)
	if err != nil {
		server.logger.Error("load response audit payload", "audit_id", audit.id, "error", err)
		return
	}
	var payload bytes.Buffer
	for _, event := range events {
		wire, err := responseEventWireData(event)
		if err != nil {
			server.logger.Error("decode response audit event", "audit_id", audit.id, "sequence", event.Sequence, "error", err)
			return
		}
		payload.Write(wire)
	}
	if payload.Len() == 0 {
		return
	}
	if _, err := server.audit.StoreAuditPayload(ctx, storeapi.AuditPayload{
		AuditID: audit.id, Kind: "response", Data: payload.Bytes(),
		CreatedAt: completedAt, ExpiresAt: completedAt.Add(server.config.AuditPayloadTTL),
	}); err != nil {
		server.logger.Error("store response audit payload", "audit_id", audit.id, "error", err)
	}
}

func (*Server) health(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	_, _ = response.Write([]byte(`{"status":"ok"}`))
}

func (server *Server) models(response http.ResponseWriter, request *http.Request) {
	principal, err := server.authenticate(request)
	if err != nil || principal.Type != auth.PrincipalCaller {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusUnauthorized)
		_, _ = response.Write([]byte(`{"error":{"message":"unauthorized","type":"authentication_error"}}`))
		return
	}
	data := make([]map[string]any, 0, len(server.config.Models))
	for _, model := range server.config.Models {
		data = append(data, map[string]any{"id": model, "object": "model", "owned_by": "human-agent"})
	}
	writeJSON(response, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (server *Server) completion(route string) http.HandlerFunc {
	codec := server.codecs[route]
	return func(response http.ResponseWriter, request *http.Request) {
		server.serveCompletion(response, request, codec)
	}
}

func (server *Server) serveCompletion(response http.ResponseWriter, request *http.Request, codec dialect.Codec) {
	principal, err := server.authenticate(request)
	if err != nil {
		server.writeAPIError(response, codec, http.StatusUnauthorized, "invalid_api_key", "invalid or revoked API token")
		return
	}
	if principal.Type != auth.PrincipalCaller {
		server.writeAPIError(response, codec, http.StatusUnauthorized, "invalid_api_key", "caller API token required")
		return
	}
	callerID := principal.SubjectID
	declaredCaller, err := validateCallerDeclaration(request.Header, callerID)
	if err != nil {
		server.writeAPIError(response, codec, http.StatusForbidden, "caller_identity_mismatch", err.Error())
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, server.config.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			server.writeAPIError(response, codec, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the worker wire limit")
		} else {
			server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", "request body is unreadable")
		}
		return
	}
	canonicalRequest, err := codec.Decode(body)
	if err != nil {
		server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	digest, err := canonicalRequest.Digest()
	if err != nil {
		server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	identity, tier, err := server.identity(request, callerID)
	if err != nil {
		server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if tier != completion.TierChat {
		if err := completeOpenCodeIdentity(&identity, request.Header, request.UserAgent(), canonicalRequest); err != nil {
			server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if err := resumeOpenCodeTask(request.Context(), server.store, &identity); err != nil {
			server.writeAPIError(response, codec, http.StatusInternalServerError, "store_error", "failed to resolve harness continuation")
			return
		}
		if identity.IdempotencyKey == "" {
			identity.IdempotencyKey, err = resolveOpenCodeIdempotencyKey(
				"", identity, tier, request.UserAgent(), canonicalRequest, body,
			)
			if err != nil {
				server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", err.Error())
				return
			}
		}
	}
	var resolvedProfile *adapter.Profile
	if tier != completion.TierChat {
		profile, known := server.adapters.Resolve(identity.HarnessID, identity.HarnessVersion)
		if known && tier == completion.TierRemoteTools && profile.HarnessID == adapter.HumanShimID && declaredCaller == "" {
			server.writeAPIError(
				response, codec, http.StatusPreconditionRequired,
				"caller_identity_required", "human-shim remote-tools requests require X-Human-Caller-Id",
			)
			return
		}
		if !known || identity.Validate(tier) != nil ||
			isOpenCodeAuxiliaryRequest(profile, canonicalRequest) {
			tier = completion.TierChat
		} else {
			resolvedProfile = &profile
		}
	}
	if tier == completion.TierChat {
		// Basic/Chat never accepts a caller-supplied stable task namespace,
		// whether it was requested directly or reached by safe downgrade. Each
		// completion owns a fresh task; request-level idempotency remains valid
		// for exact replay.
		identity.TaskID = ""
		identity.WorkspaceKey = ""
		identity.HarnessID = ""
		identity.HarnessVersion = ""
		identity.HarnessSessionID = ""
		identity.Root = ""
		identity.ExecAllowed = false
	}

	idempotencyKey := identity.IdempotencyKey
	if idempotencyKey == "" && !server.config.DisableCodexAutoIdempotency {
		idempotencyKey, err = resolveRequestIdempotencyKey(
			identity.IdempotencyKey, callerID, tier, request.Header, request.UserAgent(), canonicalRequest, body,
		)
		if err != nil {
			server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	if idempotencyKey == "" {
		idempotencyKey, err = canonical.NewOpaqueID("request_")
		if err != nil {
			server.writeAPIError(response, codec, http.StatusInternalServerError, "internal_error", "failed to allocate request id")
			return
		}
	}
	requestKey := storeapi.RequestKey{CallerID: callerID, IdempotencyKey: idempotencyKey}
	lookup, err := server.store.LookupRequest(request.Context(), requestKey, digest)
	switch {
	case err == nil:
		server.replayResponse(response, request, lookup)
		return
	case errors.Is(err, storeapi.ErrIdempotencyConflict):
		server.writeAPIError(response, codec, http.StatusConflict, "idempotency_conflict", "idempotency key was already used for a different request")
		return
	case errors.Is(err, storeapi.ErrReplayPayloadExpired):
		server.writeAPIError(response, codec, http.StatusGone, "replay_payload_expired", "exact replay payload has expired; use a new idempotency key")
		return
	case !errors.Is(err, storeapi.ErrNotFound):
		server.writeAPIError(response, codec, http.StatusInternalServerError, "store_error", "failed to inspect request state")
		return
	}
	decision := server.admissionLimiter.Allow(principal.KeyID)
	if !decision.Allowed {
		retryAfterSeconds := int64(math.Ceil(decision.RetryAfter.Seconds()))
		if retryAfterSeconds < 1 {
			retryAfterSeconds = 1
		}
		response.Header().Set("Retry-After", strconv.FormatInt(retryAfterSeconds, 10))
		response.Header().Set(headerIdempotencyKey, idempotencyKey)
		server.writeAPIError(response, codec, http.StatusTooManyRequests, "rate_limit_exceeded", "API key admission rate limit exceeded")
		return
	}

	taskID := identity.TaskID
	if taskID == "" {
		taskID, err = canonical.NewOpaqueID("task_")
		if err != nil {
			server.writeAPIError(response, codec, http.StatusInternalServerError, "internal_error", "failed to allocate task id")
			return
		}
	}
	taskKey := storeapi.TaskKey{CallerID: callerID, TaskID: taskID}
	preferredWorker := ""
	existingTask := false
	if existing, getErr := server.store.GetTask(request.Context(), taskKey); getErr == nil {
		existingTask = true
		preferredWorker = existing.LeaseOwner
		if !completion.IsStableKey(preferredWorker) {
			server.writeAPIError(response, codec, http.StatusInternalServerError, "task_owner_missing", "existing task has no valid worker owner")
			return
		}
	} else if !errors.Is(getErr, storeapi.ErrNotFound) {
		server.writeAPIError(response, codec, http.StatusInternalServerError, "store_error", "failed to inspect task state")
		return
	}
	if !existingTask && server.config.WorkerRouter != nil {
		routeIdentity := identity
		routeIdentity.TaskID = taskID
		routeIdentity.IdempotencyKey = idempotencyKey
		routeRequest := request.Clone(request.Context())
		routeRequest.Body = io.NopCloser(bytes.NewReader(body))
		routeRequest.ContentLength = int64(len(body))
		routeRequest.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
		preferredWorker, err = server.config.WorkerRouter.RouteWorker(request.Context(), WorkerRouteRequest{
			HTTPRequest: routeRequest,
			Caller:      principal,
			Model:       canonicalRequest.Model,
			Tier:        tier,
			Identity:    routeIdentity,
		})
		_ = routeRequest.Body.Close()
		switch {
		case errors.Is(err, ErrWorkerRouteDenied):
			server.writeAPIError(response, codec, http.StatusForbidden, "worker_route_denied", "worker routing policy denied this task")
			return
		case err != nil:
			server.logger.Error("route new completion task", "caller_id", callerID, "task_id", taskID, "error", err)
			server.writeAPIError(response, codec, http.StatusInternalServerError, "worker_route_error", "worker routing policy failed")
			return
		case preferredWorker != "" && !completion.IsStableKey(preferredWorker):
			server.logger.Error("worker router returned invalid subject", "caller_id", callerID, "task_id", taskID)
			server.writeAPIError(response, codec, http.StatusInternalServerError, "worker_route_error", "worker routing policy returned an invalid worker subject")
			return
		}
	}
	reservation, err := server.hub.Reserve(preferredWorker)
	if err != nil {
		status := codec.OverloadedStatus()
		code := "overloaded"
		if preferredWorker != "" && errors.Is(err, hub.ErrNoWorker) {
			code = "worker_unavailable"
		}
		response.Header().Set("Retry-After", "5")
		server.writeAPIError(response, codec, status, code, err.Error())
		return
	}
	reservationTransferred := false
	defer func() {
		if !reservationTransferred {
			reservation.Release()
		}
	}()
	workerRoot := assignmentWorkspaceRoot(tier, identity.Root, resolvedProfile)
	assignment := completion.Assignment{
		CallerID: callerID, WorkspaceKey: identity.WorkspaceKey,
		TaskID: taskID, IdempotencyKey: idempotencyKey,
		LeaseOwner:     reservation.WorkerID(),
		CapabilityTier: tier, HarnessID: identity.HarnessID, HarnessVersion: identity.HarnessVersion,
		HarnessSessionID: identity.HarnessSessionID,
		Root:             workerRoot, ExecAllowed: identity.ExecAllowed, Adapter: resolvedProfile,
		Request: canonicalRequest,
	}
	if err := workerproto.ValidateEnvelopeSize(
		workerproto.MessageAssignment, assignment, server.config.MaxWorkerMessageBytes,
	); err != nil {
		response.Header().Set(headerIdempotencyKey, idempotencyKey)
		if errors.Is(err, workerproto.ErrMessageTooLarge) {
			server.writeAPIError(
				response, codec, http.StatusRequestEntityTooLarge, "request_too_large",
				"canonical request exceeds the worker wire limit",
			)
		} else {
			server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", "request cannot be encoded for the worker")
		}
		return
	}

	begin, err := server.store.BeginRequest(request.Context(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey: taskKey, WorkspaceKey: identity.WorkspaceKey,
			CapabilityTier: tier, Dialect: codec.Dialect(),
			HarnessID: identity.HarnessID, HarnessVersion: identity.HarnessVersion,
			HarnessSessionID: identity.HarnessSessionID,
			Root:             identity.Root, ExecAllowed: identity.ExecAllowed,
			LeaseOwner: assignment.LeaseOwner,
		},
		IdempotencyKey:   idempotencyKey,
		RequestDigest:    digest,
		CanonicalRequest: canonicalRequest,
		ToolResults:      canonicalToolResults(canonicalRequest),
	})
	if err != nil {
		switch {
		case errors.Is(err, storeapi.ErrIdempotencyConflict):
			server.writeAPIError(response, codec, http.StatusConflict, "idempotency_conflict", err.Error())
		case errors.Is(err, storeapi.ErrReplayPayloadExpired):
			server.writeAPIError(response, codec, http.StatusGone, "replay_payload_expired", "exact replay payload has expired; use a new idempotency key")
		case errors.Is(err, storeapi.ErrTaskConflict), errors.Is(err, storeapi.ErrTaskNotReady),
			errors.Is(err, storeapi.ErrToolResultsMissing), errors.Is(err, storeapi.ErrToolCallConflict):
			server.writeAPIError(response, codec, http.StatusConflict, "task_reconciliation_conflict", err.Error())
		default:
			server.writeAPIError(response, codec, http.StatusInternalServerError, "store_error", "failed to admit request")
		}
		return
	}
	if begin.Replay {
		server.replayResponse(response, request, begin)
		return
	}
	audit := server.beginAudit(request.Context(), principal, begin.Task, requestKey, body)

	responseID, err := canonical.NewOpaqueID("chatcmpl_")
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "internal_error", errors.New("failed to allocate response id"))
		return
	}
	streamSeed := dialect.StreamSeed{
		CreatedAtUnix:  time.Now().UTC().Unix(),
		ToolCallPolicy: canonicalRequest.ToolCallPolicy,
	}
	metadata, err := json.Marshal(streamMetadata{
		ResponseID: responseID, Model: canonicalRequest.Model,
		CreatedAtUnix: streamSeed.CreatedAtUnix, ToolCallPolicy: streamSeed.ToolCallPolicy,
	})
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "internal_error", err)
		return
	}
	streamCheckpoint, err := server.appendResponseEvent(request.Context(), requestKey, responseEventStream, metadata)
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "store_error", err)
		return
	}
	var encoder dialect.Encoder
	if canonicalRequest.Stream {
		encoder = codec.NewStream(responseID, canonicalRequest.Model, streamSeed)
	} else {
		encoder = codec.NewAggregate(responseID, canonicalRequest.Model, streamSeed)
	}
	startFrames, err := encoder.Start()
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "response_start_failed", err)
		return
	}
	startSequence := streamCheckpoint.Sequence
	for _, frame := range startFrames {
		persisted, err := server.appendResponseEvent(request.Context(), requestKey, responseEventWire, frame)
		if err != nil {
			server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "store_error", err)
			return
		}
		startSequence = persisted.Sequence
	}

	if canonicalRequest.Stream {
		if _, ok := response.(http.Flusher); !ok {
			server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "streaming_unsupported", errors.New("HTTP response writer does not support flushing"))
			return
		}
		begin.Request, err = server.beginResponse(request.Context(), begin.Request)
		if err != nil {
			server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "store_error", fmt.Errorf("commit streaming response boundary: %w", err))
			return
		}
	}
	// BeginRequest plus the response-mode checkpoint transfer admission to the
	// daemon. Streaming keeps the stronger flush-before-Enqueue gate after its
	// durable 200 decision. Non-streaming opens the gate while HTTP is still
	// undecided; its terminal event atomically commits status plus the one body.
	// In either mode, losing this socket does not cancel durable work.
	dispatchReady := make(chan struct{})
	reservationTransferred = true
	server.startBackground(func() {
		server.dispatchCommittedRequest(
			server.runtimeContext(), dispatchReady, reservation, assignment,
			encoder, begin.Task, begin.Request, audit,
		)
	})

	if !canonicalRequest.Stream {
		close(dispatchReady)
		decided, err := server.awaitResponseDecision(request.Context(), begin)
		if err != nil {
			var ok bool
			decided, ok = server.resolveResponseDecisionAfterError(begin)
			if !ok {
				abortUndecidedHTTPResponse()
				return
			}
		}
		server.writeResponseDecision(response, decided.Task, decided.Request)
		return
	}
	cursor, err := server.beginFreshStreamingResponse(response, request, begin, startFrames, startSequence)
	close(dispatchReady)
	if err != nil {
		if request.Context().Err() == nil && server.runtimeContext().Err() == nil {
			server.logger.Error("start durable completion response", "caller_id", callerID, "idempotency_key", idempotencyKey, "error", err)
		}
		return
	}
	server.continueStreamingResponse(response, request, begin, cursor)
}

// beginFreshStreamingResponse writes the exact frames that were just made
// durable, so the 200-to-Enqueue path has no fallible store read. Request
// cancellation stops work on this particular socket; dispatchCommittedRequest
// remains owned by the daemon lifecycle.
func (server *Server) beginFreshStreamingResponse(
	response http.ResponseWriter,
	request *http.Request,
	lookup storeapi.BeginRequestResult,
	startFrames [][]byte,
	startSequence int64,
) (streamReplayCursor, error) {
	if err := request.Context().Err(); err != nil {
		return streamReplayCursor{}, err
	}
	if lookup.Request.Response.StatusCode != http.StatusOK {
		return streamReplayCursor{}, fmt.Errorf("stream response status is %d", lookup.Request.Response.StatusCode)
	}
	_, ok := response.(http.Flusher)
	if !ok {
		return streamReplayCursor{}, errors.New("HTTP response writer does not support flushing")
	}
	setSSEHeaders(response)
	response.Header().Set(headerTaskID, lookup.Task.TaskID)
	response.Header().Set(headerIdempotencyKey, lookup.Request.IdempotencyKey)
	response.WriteHeader(http.StatusOK)
	streamWriter := newStreamResponseWriter(response, server.config.StreamWriteTimeout)
	cursor := streamReplayCursor{
		flush:    streamWriter.flush,
		send:     streamWriter.writeAndFlush,
		sequence: startSequence, started: true,
	}
	if err := cursor.send(startFrames...); err != nil {
		return cursor, err
	}
	return cursor, nil
}

func (server *Server) dispatchCommittedRequest(
	ctx context.Context,
	ready <-chan struct{},
	reservation *hub.Reservation,
	assignment completion.Assignment,
	stream dialect.Encoder,
	task storeapi.Task,
	request storeapi.Request,
	audit *auditRun,
) {
	defer reservation.Release()
	// Admission assigns a concrete transport owner before the protocol state
	// advances from admitted to leased. Keep that owner in this session-local
	// task snapshot so even reject-before-accept and synthetic terminal events
	// receive an ownership-complete durable receipt. TransitionTask remains the
	// authority for when the lease becomes part of persisted task state.
	task.LeaseOwner = assignment.LeaseOwner
	select {
	case <-ctx.Done():
		return
	case <-ready:
	}
	if ctx.Err() != nil {
		return
	}
	events, err := reservation.Enqueue(assignment)
	if err != nil {
		event := completion.Event{
			Type: completion.EventUnavailable, WorkerID: workerproto.GatewayEventOwner,
			ErrorCode: "worker_unavailable", Error: err.Error(),
		}
		pendingSteps := make(map[string]persistedStep)
		_, done, persistErr := server.persistAndApplyEventWithRetry(
			ctx, stream, task, request.CanonicalRequest,
			request.RequestKey, task.State, pendingSteps, event,
		)
		if persistErr != nil {
			server.logger.Error("persist post-admission worker loss", "caller_id", request.CallerID,
				"idempotency_key", request.IdempotencyKey, "error", persistErr)
			return
		}
		if done {
			server.completeAudit(context.Background(), audit, request.RequestKey, event.ErrorCode)
		}
		return
	}
	server.consumeSession(stream, task, request, events, audit)
}

func (server *Server) consumeSession(
	stream dialect.Encoder,
	task storeapi.Task,
	request storeapi.Request,
	events <-chan *hub.Delivery,
	audit *auditRun,
) {
	ctx := server.runtimeContext()
	abortOnReturn := true
	defer func() {
		if abortOnReturn {
			_ = server.hub.Abort(request.CallerID, request.IdempotencyKey)
		}
	}()
	heartbeats := time.NewTicker(server.config.HeartbeatInterval)
	defer heartbeats.Stop()
	remaining := server.config.MaxPending - time.Since(request.CreatedAt)
	if remaining < 0 {
		remaining = 0
	}
	expiry := time.NewTimer(remaining)
	defer expiry.Stop()
	current := task.State
	pendingSteps := make(map[string]persistedStep)
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeats.C:
			heartbeat := stream.Heartbeat()
			if len(heartbeat) == 0 {
				continue
			}
			if _, err := server.appendResponseEvent(ctx, request.RequestKey, responseEventWire, heartbeat); err != nil {
				server.logger.Error("persist completion heartbeat", "caller_id", request.CallerID, "idempotency_key", request.IdempotencyKey, "error", err)
				continue
			}
		case <-expiry.C:
			event := completion.Event{
				Type: completion.EventExpired, WorkerID: workerproto.GatewayEventOwner, ErrorCode: "human_timeout",
				Error: "human response exceeded max_pending",
			}
			next, done, err := server.persistAndApplyEventWithRetry(
				ctx, stream, task, request.CanonicalRequest, request.RequestKey, current, pendingSteps, event,
			)
			if err != nil {
				server.logger.Error("expire completion session", "caller_id", request.CallerID, "idempotency_key", request.IdempotencyKey, "error", err)
				return
			}
			current = next
			_ = server.hub.Abort(request.CallerID, request.IdempotencyKey)
			abortOnReturn = false
			if done {
				server.completeAudit(context.Background(), audit, request.RequestKey, event.ErrorCode)
			}
			return
		case delivery := <-events:
			event := delivery.Event
			next, done, err := server.persistAndApplyEventWithRetry(
				ctx, stream, task, request.CanonicalRequest, request.RequestKey, current, pendingSteps, event,
			)
			commitErr := err
			var rejected *rejectableWorkerEventError
			if errors.As(err, &rejected) {
				// This event reached the durable completion processor and failed
				// validation before any of its effects became durable. Tell workerws
				// to reject exactly this outbox item; reconnecting cannot make it
				// valid and would otherwise permanently block later worker events.
				commitErr = hub.RejectEvent(err)
			}
			delivery.Commit(commitErr)
			if err != nil {
				server.logger.Error("process completion event", "caller_id", request.CallerID, "idempotency_key", request.IdempotencyKey, "event_id", event.ID, "error", err)
				continue
			}
			current = next
			if event.Type == completion.EventAccepted && audit != nil && audit.acceptedAt.IsZero() {
				audit.acceptedAt = time.Now().UTC()
			}
			if done {
				// Commit(nil) has already released the terminal publisher, which
				// owns successful session retirement and its compact replay receipt.
				abortOnReturn = false
				server.completeAudit(context.Background(), audit, request.RequestKey, event.ErrorCode)
				return
			}
		}
	}
}

type retryableEventStageError struct {
	err error
}

func (failure *retryableEventStageError) Error() string { return failure.err.Error() }
func (failure *retryableEventStageError) Unwrap() error { return failure.err }

// rejectableWorkerEventError is reserved for validation failures discovered
// before any response step or state effect is durable. Errors after that
// boundary must keep the outbox item for recovery instead of falsely NACKing an
// event whose partial commit still has to be reconciled.
type rejectableWorkerEventError struct {
	err error
}

func (failure *rejectableWorkerEventError) Error() string { return failure.err.Error() }
func (failure *rejectableWorkerEventError) Unwrap() error { return failure.err }

type invalidTaskToolCallIDError struct {
	err error
}

func (failure *invalidTaskToolCallIDError) Error() string { return failure.err.Error() }
func (failure *invalidTaskToolCallIDError) Unwrap() error { return failure.err }

func workerEventStageStoreError(operation string, err error) error {
	wrapped := fmt.Errorf("%s: %w", operation, err)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, storeapi.ErrNotFound) ||
		errors.Is(err, storeapi.ErrWorkerEventConflict) ||
		errors.Is(err, storeapi.ErrUnrecoverableRequest) ||
		errors.Is(err, storeapi.ErrTaskConflict) ||
		errors.Is(err, storeapi.ErrLeaseConflict) ||
		errors.Is(err, storeapi.ErrToolCallConflict) ||
		errors.Is(err, storeapi.ErrToolResultsMissing) ||
		errors.Is(err, storeapi.ErrTaskNotReady) {
		return wrapped
	}
	return &retryableEventStageError{err: wrapped}
}

func unrecoverableWorkerEvent(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", storeapi.ErrUnrecoverableRequest, fmt.Sprintf(format, arguments...))
}

// persistAndApplyEventWithRetry owns an event from validation through its
// durable receipt. Once a codec has advanced, returning to the session select
// would let a heartbeat, expiry, or later event overtake the pending step.
// Retrying here preserves event order and also gives synthetic terminal events
// (which have no worker outbox) an online recovery path.
func (server *Server) persistAndApplyEventWithRetry(
	ctx context.Context,
	stream dialect.Encoder,
	task storeapi.Task,
	request canonical.Request,
	requestKey storeapi.RequestKey,
	current completion.State,
	pendingSteps map[string]persistedStep,
	event completion.Event,
) (completion.State, bool, error) {
	if event.ID == "" {
		var err error
		event.ID, err = canonical.NewOpaqueID("event_")
		if err != nil {
			return current, false, fmt.Errorf("allocate worker event id: %w", err)
		}
	}
	backoff := 10 * time.Millisecond
	for {
		next, done, err := server.persistAndApplyEvent(
			ctx, stream, task, request, requestKey, current, pendingSteps, event,
		)
		if err == nil {
			return next, done, nil
		}
		var retryable *retryableEventStageError
		if !errors.As(err, &retryable) {
			return next, false, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return next, false, ctx.Err()
		case <-timer.C:
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
}

func (server *Server) persistAndApplyEvent(
	ctx context.Context,
	stream dialect.Encoder,
	task storeapi.Task,
	request canonical.Request,
	requestKey storeapi.RequestKey,
	current completion.State,
	pendingSteps map[string]persistedStep,
	event completion.Event,
) (completion.State, bool, error) {
	// The session-local owner exists from admission, before the persisted task
	// formally transitions through leased. Preserve it across the authoritative
	// task refresh below for reject-before-accept and synthetic receipts.
	assignedWorkerID := task.LeaseOwner
	if event.ID == "" {
		var err error
		event.ID, err = canonical.NewOpaqueID("event_")
		if err != nil {
			return current, false, fmt.Errorf("allocate worker event id: %w", err)
		}
	}
	eventDigest, err := workerEventDigest(event)
	if err != nil {
		return current, false, err
	}
	stages, err := server.loadWorkerEventStages(ctx, requestKey, event, eventDigest)
	if err != nil {
		return current, false, workerEventStageStoreError("load durable worker event stages", err)
	}
	latest, err := server.store.GetTask(ctx, task.TaskKey)
	if err != nil {
		return current, false, workerEventStageStoreError("refresh task before worker event", err)
	}
	task = latest
	current = latest.State

	if !stages.hasStep {
		step, cached := pendingSteps[event.ID]
		if cached {
			cachedDigest, digestErr := workerEventDigest(step.Event)
			if digestErr != nil {
				return current, false, digestErr
			}
			if cachedDigest != eventDigest {
				return current, false, storeapi.ErrWorkerEventConflict
			}
		} else {
			if err := server.validateEventState(task, request, current, event); err != nil {
				return current, false, &rejectableWorkerEventError{err: err}
			}
			if event.Type == completion.EventToolCalls {
				if err := server.validateNewToolCallIDs(ctx, task, event.ToolCalls); err != nil {
					var invalid *invalidTaskToolCallIDError
					if errors.As(err, &invalid) {
						return current, false, &rejectableWorkerEventError{err: err}
					}
					return current, false, workerEventStageStoreError("validate task tool-call ids", err)
				}
			}
			encodedAtUnix := time.Now().UTC().Unix()
			frames, done, encodeErr := stream.Encode(event, dialect.EventSeed{EncodedAtUnix: encodedAtUnix})
			if encodeErr != nil {
				return current, false, fmt.Errorf("encode worker event: %w", encodeErr)
			}
			step = persistedStep{
				Event: event, Wire: bytes.Join(frames, nil), Done: done,
				EncodedAtUnix: encodedAtUnix,
			}
			pendingSteps[event.ID] = step
		}
		stepPayload, marshalErr := json.Marshal(step)
		if marshalErr != nil {
			return current, false, fmt.Errorf("marshal durable worker event: %w", marshalErr)
		}
		if _, appendErr := server.appendWorkerResponseEvent(
			ctx, requestKey, responseEventStep, event.ID, eventDigest, stepPayload,
		); appendErr != nil {
			// A transaction may have committed even if its caller observed an
			// error. Re-read the unique checkpoint before asking the outbox to
			// retry an already-mutated codec stream.
			persisted, verifyErr := server.loadWorkerEventStages(ctx, requestKey, event, eventDigest)
			if verifyErr != nil || !persisted.hasStep {
				if verifyErr != nil {
					return current, false, workerEventStageStoreError("verify durable worker event", verifyErr)
				}
				return current, false, workerEventStageStoreError("persist worker event", appendErr)
			}
			stages = persisted
		} else {
			stages = workerEventStages{step: step, payload: stepPayload, hasStep: true}
		}
	}

	next := current
	if !stages.applied {
		next, err = server.applyEventState(ctx, task, request, current, stages.step.Event)
		if err != nil {
			return current, false, workerEventStageStoreError("apply worker event state", err)
		}
		if _, appendErr := server.appendWorkerResponseEvent(
			ctx, requestKey, responseEventApplied, event.ID, eventDigest, stages.payload,
		); appendErr != nil {
			persisted, verifyErr := server.loadWorkerEventStages(ctx, requestKey, event, eventDigest)
			if verifyErr != nil || !persisted.applied {
				if verifyErr != nil {
					return next, false, workerEventStageStoreError("verify applied worker event", verifyErr)
				}
				return next, false, workerEventStageStoreError("publish applied worker event", appendErr)
			}
			stages = persisted
		} else {
			stages.applied = true
		}
	}

	if stages.step.Done {
		var completeErr error
		var expectedDecision *storeapi.ResponseDecision
		if request.Stream {
			completeErr = server.completeRequest(ctx, requestKey)
		} else {
			decision, decisionErr := nonStreamingDecision(request.Dialect, stages.step.Event, stages.step.Wire)
			if decisionErr != nil {
				return next, false, decisionErr
			}
			expectedDecision = &decision
			_, completeErr = server.completeNonStreamingResponse(ctx, requestKey, decision)
		}
		if completeErr != nil {
			requestDigest, digestErr := request.Digest()
			if digestErr != nil {
				return next, false, digestErr
			}
			lookup, lookupErr := server.store.LookupRequest(ctx, requestKey, requestDigest)
			decisionMatches := expectedDecision == nil || responseDecisionsEqual(lookup.Request.Response, *expectedDecision)
			if lookupErr != nil || !lookup.Request.ResponseComplete || !decisionMatches {
				if lookupErr != nil {
					return next, false, workerEventStageStoreError("verify durable response completion", lookupErr)
				}
				return next, false, workerEventStageStoreError("complete durable response", completeErr)
			}
			// The Store may have committed before its caller observed an
			// error. The durable re-read above is authoritative; wake HTTP
			// waiters just as the normal wrapper path would have done.
			server.responses.notify(requestKey)
		}
	}
	receiptWorkerID := event.WorkerID
	if receiptWorkerID == "" {
		receiptWorkerID = task.LeaseOwner
	}
	if receiptWorkerID == "" {
		receiptWorkerID = assignedWorkerID
	}
	if strings.TrimSpace(receiptWorkerID) == "" {
		return next, false, unrecoverableWorkerEvent(
			"worker event %q has no durable owner for its receipt", event.ID,
		)
	}
	if _, receiptErr := server.store.RecordWorkerEventReceipt(
		ctx, requestKey, event.ID, receiptWorkerID, eventDigest,
	); receiptErr != nil {
		receipt, lookupErr := server.store.LookupWorkerEventReceipt(ctx, requestKey, event.ID)
		if lookupErr != nil || receipt.WorkerID != receiptWorkerID || receipt.Digest != eventDigest {
			return next, false, workerEventStageStoreError("record worker event receipt", receiptErr)
		}
	}
	delete(pendingSteps, event.ID)
	return next, stages.step.Done, nil
}

func nonStreamingDecision(requestDialect canonical.Dialect, event completion.Event, body []byte) (storeapi.ResponseDecision, error) {
	if len(body) == 0 {
		return storeapi.ResponseDecision{}, errors.New("terminal non-streaming event produced an empty body")
	}
	decision := storeapi.ResponseDecision{
		StatusCode: http.StatusOK, ContentType: "application/json", Body: bytes.Clone(body),
	}
	switch event.Type {
	case completion.EventFinal, completion.EventClarification, completion.EventToolCalls:
	case completion.EventRejected:
		decision.StatusCode = http.StatusConflict
	case completion.EventExpired:
		decision.StatusCode = http.StatusGatewayTimeout
	case completion.EventUnavailable:
		if requestDialect == canonical.DialectAnthropic {
			decision.StatusCode = 529
		} else {
			decision.StatusCode = http.StatusServiceUnavailable
		}
		decision.RetryAfter = "5"
	case completion.EventFailed:
		decision.StatusCode = http.StatusInternalServerError
	default:
		return storeapi.ResponseDecision{}, fmt.Errorf("non-streaming response ended with unsupported event %q", event.Type)
	}
	return decision, nil
}

func responseDecisionsEqual(left, right storeapi.ResponseDecision) bool {
	return left.StatusCode == right.StatusCode && left.ContentType == right.ContentType &&
		left.RetryAfter == right.RetryAfter && bytes.Equal(left.Body, right.Body)
}

type workerEventStages struct {
	step    persistedStep
	payload []byte
	hasStep bool
	applied bool
}

func validateIndexedWorkerEvent(stored storeapi.ResponseEvent, event completion.Event) error {
	if stored.EventID != "" && stored.EventID != event.ID {
		return fmt.Errorf(
			"%w: indexed worker event %q contains %q",
			storeapi.ErrUnrecoverableRequest, stored.EventID, event.ID,
		)
	}
	digest, err := workerEventDigest(event)
	if err != nil {
		return err
	}
	if stored.EventDigest != "" && stored.EventDigest != digest {
		return storeapi.ErrWorkerEventConflict
	}
	return nil
}

func validPersistedStep(step persistedStep) bool {
	return step.Event.ID != "" && step.EncodedAtUnix > 0
}

func (server *Server) loadWorkerEventStages(
	ctx context.Context,
	requestKey storeapi.RequestKey,
	event completion.Event,
	eventDigest string,
) (workerEventStages, error) {
	events, err := server.store.ListWorkerEventStages(ctx, requestKey, event.ID)
	if err != nil {
		return workerEventStages{}, fmt.Errorf("list durable worker event stages: %w", err)
	}
	var stages workerEventStages
	var appliedPayload []byte
	for _, stored := range events {
		if stored.Kind != responseEventStep && stored.Kind != responseEventApplied {
			continue
		}
		var step persistedStep
		if err := json.Unmarshal(stored.Data, &step); err != nil || !validPersistedStep(step) {
			return workerEventStages{}, fmt.Errorf(
				"%w: invalid worker event stage at sequence %d",
				storeapi.ErrUnrecoverableRequest, stored.Sequence,
			)
		}
		if err := validateIndexedWorkerEvent(stored, step.Event); err != nil {
			return workerEventStages{}, err
		}
		if step.Event.ID != event.ID {
			continue
		}
		digest, err := workerEventDigest(step.Event)
		if err != nil {
			return workerEventStages{}, err
		}
		if digest != eventDigest {
			return workerEventStages{}, storeapi.ErrWorkerEventConflict
		}
		switch stored.Kind {
		case responseEventStep:
			if stages.hasStep {
				return workerEventStages{}, fmt.Errorf(
					"%w: duplicate durable response step %q",
					storeapi.ErrUnrecoverableRequest, event.ID,
				)
			}
			stages.step = step
			stages.payload = bytes.Clone(stored.Data)
			stages.hasStep = true
		case responseEventApplied:
			if stages.applied {
				return workerEventStages{}, fmt.Errorf(
					"%w: duplicate applied response step %q",
					storeapi.ErrUnrecoverableRequest, event.ID,
				)
			}
			stages.applied = true
			appliedPayload = bytes.Clone(stored.Data)
		}
	}
	if stages.applied && !stages.hasStep {
		return workerEventStages{}, fmt.Errorf(
			"%w: applied response step %q has no durable event",
			storeapi.ErrUnrecoverableRequest, event.ID,
		)
	}
	if stages.applied && !bytes.Equal(stages.payload, appliedPayload) {
		return workerEventStages{}, fmt.Errorf(
			"%w: applied response step %q differs from its durable event",
			storeapi.ErrUnrecoverableRequest, event.ID,
		)
	}
	return stages, nil
}

func (server *Server) validateEventState(
	task storeapi.Task,
	request canonical.Request,
	current completion.State,
	event completion.Event,
) error {
	switch event.Type {
	case completion.EventAccepted:
		if current != completion.StateAdmitted && current != completion.StateReconciled {
			return fmt.Errorf("worker accepted task in state %q", current)
		}
	case completion.EventProgress:
		if current != completion.StateAwaitingHuman {
			return fmt.Errorf("progress received in state %q", current)
		}
	case completion.EventFinal, completion.EventClarification, completion.EventToolCalls:
		if current != completion.StateAwaitingHuman {
			return fmt.Errorf("response received in state %q", current)
		}
		if event.Type == completion.EventToolCalls {
			return server.validateToolCalls(task, request, event.ToolCalls)
		}
	case completion.EventRejected, completion.EventExpired, completion.EventFailed, completion.EventUnavailable:
		if current.IsTerminal() {
			return fmt.Errorf("terminal event received in state %q", current)
		}
	default:
		return fmt.Errorf("unsupported worker event %q", event.Type)
	}
	return nil
}

func (server *Server) applyEventState(
	ctx context.Context,
	task storeapi.Task,
	request canonical.Request,
	current completion.State,
	event completion.Event,
) (completion.State, error) {
	transition := func(next completion.State, workerID string) error {
		updated, err := server.store.TransitionTask(ctx, task.TaskKey, current, next, workerID)
		if err == nil {
			current = updated.State
			task.LeaseOwner = updated.LeaseOwner
		}
		return err
	}
	switch event.Type {
	case completion.EventAccepted:
		switch current {
		case completion.StateAdmitted, completion.StateReconciled:
			if err := transition(completion.StateLeased, event.WorkerID); err != nil {
				return current, err
			}
			if err := transition(completion.StateAwaitingHuman, ""); err != nil {
				return current, err
			}
		case completion.StateLeased:
			if err := transition(completion.StateAwaitingHuman, ""); err != nil {
				return current, err
			}
		default:
			// An accepted step may already have been applied before a crash.
			if !current.Valid() {
				return current, unrecoverableWorkerEvent("worker accepted task in state %q", current)
			}
		}
	case completion.EventProgress:
		// Progress has no state effect. Persisted progress may be replayed
		// after the task has advanced further.
	case completion.EventFinal, completion.EventClarification, completion.EventToolCalls:
		if event.Type == completion.EventToolCalls {
			if err := server.validateToolCalls(task, request, event.ToolCalls); err != nil {
				return current, unrecoverableWorkerEvent("invalid durable tool calls: %v", err)
			}
		}
		if current == completion.StateAwaitingHuman {
			if err := transition(completion.StateResponded, ""); err != nil {
				return current, err
			}
		}
		switch event.Type {
		case completion.EventFinal:
			if current == completion.StateCompleted {
				break
			}
			if current != completion.StateResponded {
				return current, unrecoverableWorkerEvent("final response cannot resume from state %q", current)
			}
			if err := transition(completion.StateCompleted, ""); err != nil {
				return current, err
			}
		case completion.EventClarification:
			next := completion.StateAwaitingCaller
			if task.CapabilityTier == completion.TierChat {
				// Chat clients observe clarification as a normal stop response and
				// carry any user's follow-up in a new, independent completion.
				next = completion.StateCompleted
			}
			if current == next {
				break
			}
			if current != completion.StateResponded {
				return current, unrecoverableWorkerEvent("clarification cannot resume from state %q", current)
			}
			if err := transition(next, ""); err != nil {
				return current, err
			}
		case completion.EventToolCalls:
			// Chat clients execute their own declared tools and submit the result
			// in a new, independent completion. They do not carry our stable
			// task identity across requests, so this response is terminal and
			// must not create caller-shim execution records that can never be
			// reconciled. Adapter-gated tiers retain the durable tool loop below.
			if task.CapabilityTier == completion.TierChat {
				if current == completion.StateCompleted {
					break
				}
				if current != completion.StateResponded {
					return current, unrecoverableWorkerEvent("Chat tool response cannot resume from state %q", current)
				}
				if err := transition(completion.StateCompleted, ""); err != nil {
					return current, err
				}
				break
			}
			for _, call := range event.ToolCalls {
				digest, err := toolCallDigest(call)
				if err != nil {
					return current, unrecoverableWorkerEvent("digest durable tool call %q: %v", call.ID, err)
				}
				if _, err := server.store.BeginToolExecution(ctx, storeapi.ToolExecutionKey{
					CallerID: task.CallerID, TaskID: task.TaskID, ToolCallID: call.ID,
				}, digest); err != nil {
					return current, err
				}
			}
			if current == completion.StateResponded {
				if err := transition(completion.StateToolsDispatched, ""); err != nil {
					return current, err
				}
			}
			if current == completion.StateToolsDispatched {
				if err := transition(completion.StateAwaitingResults, ""); err != nil {
					return current, err
				}
			}
			if current != completion.StateAwaitingResults {
				return current, unrecoverableWorkerEvent("tool response cannot resume from state %q", current)
			}
		}
	case completion.EventRejected:
		if current == completion.StateRejected {
			break
		}
		if current.IsTerminal() {
			return current, unrecoverableWorkerEvent("rejected response conflicts with terminal state %q", current)
		}
		if err := transition(completion.StateRejected, ""); err != nil {
			return current, err
		}
	case completion.EventExpired:
		if current == completion.StateExpired {
			break
		}
		if current.IsTerminal() {
			return current, unrecoverableWorkerEvent("expired response conflicts with terminal state %q", current)
		}
		if err := transition(completion.StateExpired, ""); err != nil {
			return current, err
		}
	case completion.EventFailed, completion.EventUnavailable:
		if current == completion.StateFailed {
			break
		}
		if current.IsTerminal() {
			return current, unrecoverableWorkerEvent("failed response conflicts with terminal state %q", current)
		}
		if err := transition(completion.StateFailed, ""); err != nil {
			return current, err
		}
	default:
		return current, unrecoverableWorkerEvent("unsupported worker event %q", event.Type)
	}
	return current, nil
}

func (server *Server) validateToolCalls(task storeapi.Task, request canonical.Request, calls []completion.ToolCall) error {
	if request.ToolCallPolicy == canonical.ToolCallsSerial && len(calls) > 1 {
		return fmt.Errorf("serial tool-call policy allows one call, got %d", len(calls))
	}
	type toolIdentity struct{ namespace, name string }
	declared := make(map[toolIdentity]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		declared[toolIdentity{namespace: tool.Namespace, name: tool.Name}] = struct{}{}
	}
	seen := make(map[string]struct{}, len(calls))
	for _, call := range calls {
		if strings.TrimSpace(call.ID) == "" || call.ID != strings.TrimSpace(call.ID) {
			return errors.New("tool calls require stable ids and names")
		}
		if err := canonical.ValidateToolIdentity(call.Namespace, call.Name); err != nil {
			return fmt.Errorf("tool call %q has invalid identity: %w", call.ID, err)
		}
		if _, duplicate := seen[call.ID]; duplicate {
			return fmt.Errorf("duplicate tool call id %q", call.ID)
		}
		seen[call.ID] = struct{}{}
		identity := toolIdentity{namespace: call.Namespace, name: call.Name}
		if _, ok := declared[identity]; !ok {
			return fmt.Errorf("tool %q was not declared by the caller", call.QualifiedName())
		}
	}
	if len(calls) == 0 {
		return errors.New("tool_calls response contains no calls")
	}
	if task.CapabilityTier == completion.TierChat {
		return nil
	}
	profile, ok := server.adapters.Resolve(task.HarnessID, task.HarnessVersion)
	if !ok {
		return errors.New("task has no exact harness adapter")
	}
	for _, call := range calls {
		var authorization adapter.ToolAuthorization
		var classified bool
		if call.Namespace == "" {
			authorization, classified = profile.AuthorizeTool(call.Name)
		}
		if (!classified || authorization == adapter.ToolAuthorizationPrivileged) && !task.ExecAllowed {
			if !classified {
				return fmt.Errorf("unclassified caller tool %q requires explicit authorization (X-Human-Allow-Exec: true)", call.QualifiedName())
			}
			return fmt.Errorf("privileged caller tool %q requires explicit authorization (X-Human-Allow-Exec: true)", call.QualifiedName())
		}
	}
	return nil
}

// validateNewToolCallIDs enforces task-wide tool-call identity before the
// response step becomes durable. A worker event may be replayed with the same
// event ID and digest through the durable event-stage path, but a distinct
// event must never reuse a tool_call_id: doing so would make a later caller
// result ambiguous and used to leave the rejected event at the head of the
// worker outbox forever.
func (server *Server) validateNewToolCallIDs(
	ctx context.Context,
	task storeapi.Task,
	calls []completion.ToolCall,
) error {
	if task.CapabilityTier == completion.TierChat {
		return nil
	}
	for _, call := range calls {
		digest, err := toolCallDigest(call)
		if err != nil {
			return &invalidTaskToolCallIDError{err: fmt.Errorf(
				"tool call %q cannot be encoded: %w", call.ID, err,
			)}
		}
		execution, err := server.store.LookupToolExecution(ctx, storeapi.ToolExecutionKey{
			CallerID: task.CallerID, TaskID: task.TaskID, ToolCallID: call.ID,
		})
		if errors.Is(err, storeapi.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if execution.RequestDigest == digest {
			return &invalidTaskToolCallIDError{err: fmt.Errorf(
				"%w: tool call id %q was already dispatched for this task",
				storeapi.ErrToolCallConflict, call.ID,
			)}
		}
		return &invalidTaskToolCallIDError{err: fmt.Errorf(
			"%w: tool call id %q was reused with different input",
			storeapi.ErrToolCallConflict, call.ID,
		)}
	}
	return nil
}

// Recover reconstructs every trustworthy, incomplete completion session before
// gateway starts accepting traffic. Record-local corruption is quarantined
// into a durable terminal response so unrelated sessions can still recover.
// If that terminal response cannot be persisted, recovery aborts: accepting
// traffic would leave the affected idempotency key attached to an incomplete
// response with no finite replay.
func (server *Server) Recover(ctx context.Context) error {
	if ctx == nil {
		return errors.New("recovery context is required")
	}
	server.runMu.Lock()
	server.runContext = ctx
	server.runMu.Unlock()

	snapshot, err := server.store.ListRecoverableRequests(ctx)
	if err != nil {
		return fmt.Errorf("list recoverable completion requests: %w", err)
	}
	for _, issue := range snapshot.Issues {
		if err := server.quarantineRecoveryIssue(ctx, issue); err != nil {
			return fmt.Errorf(
				"persist recovery quarantine for %s/%s: %w",
				issue.CallerID, issue.IdempotencyKey, err,
			)
		}
		server.logRecoveryQuarantine(issue.RequestKey, issue.Err)
	}
	for _, request := range snapshot.Requests {
		if err := server.recoverRequest(ctx, request); err != nil {
			if errors.Is(err, storeapi.ErrUnrecoverableRequest) {
				if terminalErr := server.failClosedRecoveryRequest(ctx, request); terminalErr != nil {
					return fmt.Errorf(
						"persist recovery failure for %s/%s after unrecoverable request (%v): %w",
						request.Request.CallerID,
						request.Request.IdempotencyKey,
						err,
						terminalErr,
					)
				}
				server.logger.Error(
					"failed closed unrecoverable completion request",
					"caller_id", request.Request.CallerID,
					"idempotency_key", request.Request.IdempotencyKey,
					"error", err,
				)
				continue
			}
			return fmt.Errorf(
				"recover completion request %s/%s: %w",
				request.Request.CallerID, request.Request.IdempotencyKey, err,
			)
		}
	}
	return nil
}

func (server *Server) quarantineRecoveryIssue(
	ctx context.Context,
	issue storeapi.RecoveryIssue,
) error {
	const message = "durable completion could not be recovered safely; start a new request identity"
	failure := storeapi.ResponseDecision{
		StatusCode:  http.StatusInternalServerError,
		ContentType: "application/json",
		Body: []byte(
			`{"error":{"type":"server_error","code":"recovery_failed","message":"` + message + `"}}`,
		),
	}
	var terminal []byte
	if codec, ok := server.codecForDialect(issue.Dialect); ok {
		failure.Body = codec.AdmissionError(
			http.StatusInternalServerError, "recovery_failed", message,
		)
		if issue.ResponseStatus == http.StatusOK {
			metadata := streamMetadata{
				ResponseID: recoveryFailureResponseID(issue.RequestKey, issue.Dialect),
				Model:      "human-expert", CreatedAtUnix: 1,
			}
			// The opaque checkpoint is optional: corrupt/missing metadata must not
			// prevent finite quarantine. When valid, preserve the response identity
			// already visible on the committed stream.
			var stored streamMetadata
			if json.Unmarshal(issue.StreamMetadata, &stored) == nil {
				if strings.TrimSpace(stored.ResponseID) != "" {
					metadata.ResponseID = stored.ResponseID
				}
				if strings.TrimSpace(stored.Model) != "" {
					metadata.Model = stored.Model
				}
				if stored.CreatedAtUnix > 0 {
					metadata.CreatedAtUnix = stored.CreatedAtUnix
				}
				metadata.ToolCallPolicy = stored.ToolCallPolicy
			}
			encoder := codec.NewStream(
				metadata.ResponseID, metadata.Model,
				dialect.StreamSeed{
					CreatedAtUnix: metadata.CreatedAtUnix, ToolCallPolicy: metadata.ToolCallPolicy,
				},
			)
			if _, err := encoder.Start(); err != nil {
				return fmt.Errorf("initialize recovery quarantine stream: %w", err)
			}
			frames, done, err := encoder.Encode(completion.Event{
				ID:        "recovery_failed_" + stableRecoverySuffix(issue.RequestKey),
				Type:      completion.EventFailed,
				ErrorCode: "recovery_failed",
				Error:     "recovery_failed: durable completion could not be recovered safely",
			}, dialect.EventSeed{EncodedAtUnix: metadata.CreatedAtUnix})
			if err != nil {
				return fmt.Errorf("encode recovery quarantine terminal: %w", err)
			}
			if !done || len(frames) == 0 {
				return errors.New("recovery quarantine codec produced no terminal frame")
			}
			terminal = bytes.Join(frames, nil)
			if issue.Dialect == canonical.DialectResponses {
				events, err := server.store.ListResponseEvents(ctx, issue.RequestKey, 0)
				if err != nil {
					return fmt.Errorf("read Responses quarantine sequence: %w", err)
				}
				terminal, err = resequenceResponsesRecoveryTerminal(terminal, events)
				if err != nil {
					return err
				}
			}
		}
	} else if issue.ResponseStatus == http.StatusOK {
		// An unknown/corrupt task dialect cannot be made dialect-perfect. A
		// standard SSE error is still deterministic and finite, and preserves the
		// already-committed 200 transport boundary.
		terminal = []byte(
			"event: error\n" +
				`data: {"error":{"type":"server_error","code":"recovery_failed","message":"` +
				"durable completion could not be recovered safely" + `"}}` + "\n\n",
		)
	}
	return server.store.QuarantineRecoveryRequest(ctx, storeapi.RecoveryQuarantine{
		RequestKey: issue.RequestKey, Failure: failure, StreamTerminal: terminal,
	})
}

// resequenceResponsesRecoveryTerminal keeps response.failed strictly after
// any durable deltas emitted before canonical_request became unreadable. The
// fresh encoder starts at sequence 2; a partially streamed response may be
// further ahead even though reconstructing its full semantic output is no
// longer safe.
func resequenceResponsesRecoveryTerminal(
	terminal []byte,
	events []storeapi.ResponseEvent,
) ([]byte, error) {
	maxSequence := int64(-1)
	for _, event := range events {
		wire, err := responseEventWireData(event)
		if err != nil {
			return nil, fmt.Errorf("inspect durable Responses recovery event: %w", err)
		}
		for _, block := range bytes.Split(wire, []byte("\n\n")) {
			for _, line := range bytes.Split(block, []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("data: ")) {
					continue
				}
				decoder := json.NewDecoder(bytes.NewReader(bytes.TrimPrefix(line, []byte("data: "))))
				decoder.UseNumber()
				var payload struct {
					SequenceNumber json.Number `json:"sequence_number"`
				}
				if decoder.Decode(&payload) != nil || payload.SequenceNumber == "" {
					continue
				}
				sequence, err := payload.SequenceNumber.Int64()
				if err == nil && sequence > maxSequence {
					maxSequence = sequence
				}
			}
		}
	}
	dataPrefix := []byte("data: ")
	dataStart := bytes.Index(terminal, dataPrefix)
	if dataStart < 0 {
		return nil, errors.New("Responses recovery terminal has no data payload")
	}
	dataStart += len(dataPrefix)
	dataEndOffset := bytes.Index(terminal[dataStart:], []byte("\n\n"))
	if dataEndOffset < 0 {
		return nil, errors.New("Responses recovery terminal is not a complete SSE event")
	}
	dataEnd := dataStart + dataEndOffset
	decoder := json.NewDecoder(bytes.NewReader(terminal[dataStart:dataEnd]))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode Responses recovery terminal: %w", err)
	}
	payload["sequence_number"] = maxSequence + 1
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode Responses recovery terminal: %w", err)
	}
	result := make([]byte, 0, len(terminal)-dataEnd+dataStart+len(encoded))
	result = append(result, terminal[:dataStart]...)
	result = append(result, encoded...)
	result = append(result, terminal[dataEnd:]...)
	return result, nil
}

// failClosedRecoveryRequest converts a record-local reconstruction failure
// into a durable terminal response whenever its canonical request and task
// identity are still trustworthy. That preserves isolation without leaving an
// idempotency key attached to an endless/incomplete replay.
func (server *Server) failClosedRecoveryRequest(
	ctx context.Context,
	recovered storeapi.BeginRequestResult,
) error {
	codec, ok := server.codecForDialect(recovered.Task.Dialect)
	if !ok {
		return fmt.Errorf("no codec for dialect %q", recovered.Task.Dialect)
	}
	decision := storeapi.ResponseDecision{
		StatusCode: http.StatusInternalServerError, ContentType: "application/json",
		Body: codec.AdmissionError(
			http.StatusInternalServerError,
			"recovery_failed",
			"durable completion could not be recovered safely; start a new request identity",
		),
	}
	// Before phase B, preserve the task for an explicit new-key retry and make
	// the old key replay one immutable 500 response.
	if recovered.Request.Response.StatusCode == 0 &&
		(recovered.Task.State == completion.StateAdmitted || recovered.Task.State == completion.StateReconciled) {
		_, err := server.failRequest(ctx, recovered.Request.RequestKey, recovered.Task.State, decision)
		return err
	}

	request := recovered.Request.CanonicalRequest
	seedUnix := recovered.Request.CreatedAt.UTC().Unix()
	if seedUnix <= 0 {
		seedUnix = 1
	}
	responseID := recoveryFailureResponseID(recovered.Request.RequestKey, request.Dialect)
	seed := dialect.StreamSeed{CreatedAtUnix: seedUnix, ToolCallPolicy: request.ToolCallPolicy}
	var encoder dialect.Encoder
	if request.Stream {
		encoder = codec.NewStream(responseID, request.Model, seed)
	} else {
		encoder = codec.NewAggregate(responseID, request.Model, seed)
	}
	// Some encoders initialize lifecycle state in Start. Existing verified wire
	// remains untouched; only the deterministic failure step below is appended.
	if _, err := encoder.Start(); err != nil {
		return fmt.Errorf("initialize recovery failure encoder: %w", err)
	}
	if request.Stream && recovered.Request.Response.StatusCode == 0 {
		decided, err := server.beginResponse(ctx, recovered.Request)
		if err != nil {
			return fmt.Errorf("commit recovery failure stream boundary: %w", err)
		}
		recovered.Request = decided
	}
	event := completion.Event{
		ID:        "recovery_failed_" + stableRecoverySuffix(recovered.Request.RequestKey),
		Type:      completion.EventFailed,
		WorkerID:  workerproto.GatewayEventOwner,
		ErrorCode: "recovery_failed",
		Error:     "durable completion could not be recovered safely",
	}
	_, done, err := server.persistAndApplyEvent(
		ctx, encoder, recovered.Task, request, recovered.Request.RequestKey,
		recovered.Task.State, make(map[string]persistedStep), event,
	)
	if err != nil {
		return err
	}
	if !done {
		return errors.New("recovery failure event did not terminate response")
	}
	return nil
}

func stableRecoverySuffix(key storeapi.RequestKey) string {
	digest := sha256.Sum256([]byte(key.CallerID + "\x00" + key.IdempotencyKey))
	return hex.EncodeToString(digest[:12])
}

func recoveryFailureResponseID(key storeapi.RequestKey, requestDialect canonical.Dialect) string {
	prefix := "response_recovery_"
	switch requestDialect {
	case canonical.DialectOpenAIChat:
		prefix = "chatcmpl_recovery_"
	case canonical.DialectResponses:
		prefix = "resp_recovery_"
	case canonical.DialectAnthropic:
		prefix = "msg_recovery_"
	}
	return prefix + stableRecoverySuffix(key)
}

func (server *Server) logRecoveryQuarantine(key storeapi.RequestKey, err error) {
	server.logger.Error(
		"quarantined unrecoverable completion request",
		"caller_id", key.CallerID,
		"idempotency_key", key.IdempotencyKey,
		"error", err,
	)
}

func (server *Server) recoverRequest(ctx context.Context, recovered storeapi.BeginRequestResult) error {
	codec, ok := server.codecForDialect(recovered.Task.Dialect)
	if !ok {
		return fmt.Errorf("%w: no codec for dialect %q", storeapi.ErrUnrecoverableRequest, recovered.Task.Dialect)
	}
	request := recovered.Request.CanonicalRequest
	if request.Dialect != recovered.Task.Dialect {
		return fmt.Errorf(
			"%w: canonical dialect %q does not match task dialect %q",
			storeapi.ErrUnrecoverableRequest, request.Dialect, recovered.Task.Dialect,
		)
	}
	events, err := server.store.ListResponseEvents(ctx, recovered.Request.RequestKey, 0)
	if err != nil {
		return err
	}
	var metadata *streamMetadata
	startWireFrames := make([][]byte, 0)
	steps := make([]persistedStep, 0)
	appliedSteps := make(map[string]persistedStep)
	for _, event := range events {
		switch event.Kind {
		case responseEventStream:
			if metadata != nil {
				return fmt.Errorf("%w: duplicate stream metadata", storeapi.ErrUnrecoverableRequest)
			}
			var decoded streamMetadata
			if err := json.Unmarshal(event.Data, &decoded); err != nil || decoded.ResponseID == "" ||
				decoded.Model == "" || decoded.CreatedAtUnix <= 0 {
				return fmt.Errorf("%w: invalid stream metadata", storeapi.ErrUnrecoverableRequest)
			}
			metadata = &decoded
		case responseEventWire:
			startWireFrames = append(startWireFrames, event.Data)
		case responseEventStep:
			var step persistedStep
			if err := json.Unmarshal(event.Data, &step); err != nil || !validPersistedStep(step) {
				return fmt.Errorf("%w: invalid durable response step at sequence %d", storeapi.ErrUnrecoverableRequest, event.Sequence)
			}
			if err := validateIndexedWorkerEvent(event, step.Event); err != nil {
				return err
			}
			steps = append(steps, step)
		case responseEventApplied:
			var step persistedStep
			if err := json.Unmarshal(event.Data, &step); err != nil || !validPersistedStep(step) {
				return fmt.Errorf("%w: invalid applied response step at sequence %d", storeapi.ErrUnrecoverableRequest, event.Sequence)
			}
			if err := validateIndexedWorkerEvent(event, step.Event); err != nil {
				return err
			}
			if _, duplicate := appliedSteps[step.Event.ID]; duplicate {
				return fmt.Errorf("%w: duplicate applied response step %q", storeapi.ErrUnrecoverableRequest, step.Event.ID)
			}
			appliedSteps[step.Event.ID] = step
		default:
			return fmt.Errorf("%w: unknown response event kind %q", storeapi.ErrUnrecoverableRequest, event.Kind)
		}
	}
	if metadata == nil && len(events) == 0 &&
		(recovered.Task.State == completion.StateAdmitted || recovered.Task.State == completion.StateReconciled) {
		responseID, err := canonical.NewOpaqueID("chatcmpl_")
		if err != nil {
			return err
		}
		created := streamMetadata{
			ResponseID: responseID, Model: request.Model,
			CreatedAtUnix: time.Now().UTC().Unix(), ToolCallPolicy: request.ToolCallPolicy,
		}
		payload, err := json.Marshal(created)
		if err != nil {
			return err
		}
		if _, err := server.appendResponseEvent(ctx, recovered.Request.RequestKey, responseEventStream, payload); err != nil {
			return fmt.Errorf("create recovery stream checkpoint: %w", err)
		}
		metadata = &created
	}
	if metadata == nil {
		return fmt.Errorf(
			"%w: request has no recoverable stream checkpoint",
			storeapi.ErrUnrecoverableRequest,
		)
	}
	if metadata.Model != request.Model {
		return fmt.Errorf(
			"%w: stream model %q does not match canonical model %q",
			storeapi.ErrUnrecoverableRequest, metadata.Model, request.Model,
		)
	}
	seed := dialect.StreamSeed{
		CreatedAtUnix: metadata.CreatedAtUnix, ToolCallPolicy: request.ToolCallPolicy,
	}
	var encoder dialect.Encoder
	if request.Stream {
		encoder = codec.NewStream(metadata.ResponseID, metadata.Model, seed)
	} else {
		encoder = codec.NewAggregate(metadata.ResponseID, metadata.Model, seed)
	}
	startFrames, err := encoder.Start()
	if err != nil {
		return fmt.Errorf("restore response encoder start: %w", err)
	}
	if request.Stream {
		persistedStartCount := min(len(startWireFrames), len(startFrames))
		for index := 0; index < persistedStartCount; index++ {
			if !bytes.Equal(startWireFrames[index], startFrames[index]) {
				return fmt.Errorf(
					"%w: durable stream start frame %d does not match its deterministic encoding",
					storeapi.ErrUnrecoverableRequest, index,
				)
			}
		}
		if len(startWireFrames) < len(startFrames) {
			if len(steps) != 0 || len(appliedSteps) != 0 ||
				recovered.Request.Response.StatusCode != 0 ||
				recovered.Task.State != completion.StateAdmitted && recovered.Task.State != completion.StateReconciled {
				return fmt.Errorf(
					"%w: stream checkpoint contains only %d of %d start frames",
					storeapi.ErrUnrecoverableRequest, len(startWireFrames), len(startFrames),
				)
			}
			// Start may contain multiple protocol lifecycle frames. A crash can
			// occur between their individual durable appends, before phase B or
			// dispatch becomes observable. The existing frames were verified as
			// an exact prefix above, so append only the missing deterministic tail.
			if err := server.persistFrames(
				ctx, recovered.Request.RequestKey, startFrames[len(startWireFrames):],
			); err != nil {
				return fmt.Errorf("complete recovery stream start: %w", err)
			}
		}
	}
	if !request.Stream && (len(startWireFrames) != 0 || len(startFrames) != 0) {
		return fmt.Errorf("%w: non-streaming response contains stream wire", storeapi.ErrUnrecoverableRequest)
	}
	if request.Stream {
		if recovered.Request.Response.StatusCode == 0 {
			decided, err := server.beginResponse(ctx, recovered.Request)
			if err != nil {
				return fmt.Errorf("commit recovered streaming response boundary: %w", err)
			}
			recovered.Request = decided
		} else if recovered.Request.Response.StatusCode != http.StatusOK {
			return fmt.Errorf(
				"%w: incomplete request has terminal HTTP status %d",
				storeapi.ErrUnrecoverableRequest, recovered.Request.Response.StatusCode,
			)
		}
	} else if recovered.Request.ResponseComplete {
		if recovered.Request.Response.StatusCode == 0 || len(recovered.Request.Response.Body) == 0 {
			return fmt.Errorf("%w: completed non-streaming request has no HTTP decision", storeapi.ErrUnrecoverableRequest)
		}
	} else if recovered.Request.Response.StatusCode != 0 {
		return fmt.Errorf(
			"%w: incomplete non-streaming request already has HTTP status %d",
			storeapi.ErrUnrecoverableRequest, recovered.Request.Response.StatusCode,
		)
	}
	current := recovered.Task.State
	seenEventIDs := make([]string, 0, len(steps))
	knownSteps := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if _, duplicate := knownSteps[step.Event.ID]; duplicate {
			return fmt.Errorf("%w: duplicate durable response step %q", storeapi.ErrUnrecoverableRequest, step.Event.ID)
		}
		knownSteps[step.Event.ID] = struct{}{}
	}
	for eventID := range appliedSteps {
		if _, ok := knownSteps[eventID]; !ok {
			return fmt.Errorf("%w: applied response step %q has no durable event", storeapi.ErrUnrecoverableRequest, eventID)
		}
	}
	for index, step := range steps {
		frames, done, err := encoder.Encode(step.Event, dialect.EventSeed{
			EncodedAtUnix: step.EncodedAtUnix,
		})
		if err != nil {
			return fmt.Errorf("restore stream step %d: %w", index, err)
		}
		if done != step.Done {
			return fmt.Errorf("%w: response step %q completion marker disagrees with codec", storeapi.ErrUnrecoverableRequest, step.Event.ID)
		}
		if rebuilt := bytes.Join(frames, nil); !bytes.Equal(rebuilt, step.Wire) {
			return fmt.Errorf(
				"%w: response step %q wire disagrees with codec",
				storeapi.ErrUnrecoverableRequest, step.Event.ID,
			)
		}
		if applied, ok := appliedSteps[step.Event.ID]; ok {
			if applied.Done != step.Done || applied.EncodedAtUnix != step.EncodedAtUnix ||
				!bytes.Equal(applied.Wire, step.Wire) {
				return fmt.Errorf("%w: applied response step %q differs from its durable event", storeapi.ErrUnrecoverableRequest, step.Event.ID)
			}
		} else {
			// Applied is the durable proof that all state effects for this step
			// already committed. Replaying those effects from the task's latest
			// state can corrupt a later completion in the same task; only a step
			// missing Applied is eligible for state resumption.
			current, err = server.applyEventState(ctx, recovered.Task, request, current, step.Event)
			if err != nil {
				return fmt.Errorf("resume durable response step %q: %w", step.Event.ID, err)
			}
			payload, err := json.Marshal(step)
			if err != nil {
				return err
			}
			eventDigest, err := workerEventDigest(step.Event)
			if err != nil {
				return err
			}
			if _, err := server.appendWorkerResponseEvent(
				ctx, recovered.Request.RequestKey, responseEventApplied,
				step.Event.ID, eventDigest, payload,
			); err != nil {
				return fmt.Errorf("publish recovered response step %q: %w", step.Event.ID, err)
			}
		}
		eventDigest, err := workerEventDigest(step.Event)
		if err != nil {
			return err
		}
		if step.Done {
			if index != len(steps)-1 {
				return fmt.Errorf("%w: durable response contains events after terminal step", storeapi.ErrUnrecoverableRequest)
			}
			if request.Stream {
				if err := server.completeRequest(ctx, recovered.Request.RequestKey); err != nil {
					return err
				}
			} else {
				decision, err := nonStreamingDecision(request.Dialect, step.Event, step.Wire)
				if err != nil {
					return err
				}
				if recovered.Request.ResponseComplete {
					if !responseDecisionsEqual(recovered.Request.Response, decision) {
						return fmt.Errorf(
							"%w: completed non-streaming response disagrees with terminal step",
							storeapi.ErrUnrecoverableRequest,
						)
					}
				} else if _, err := server.completeNonStreamingResponse(ctx, recovered.Request.RequestKey, decision); err != nil {
					return err
				}
			}
		}
		receiptWorkerID := step.Event.WorkerID
		if receiptWorkerID == "" {
			receiptWorkerID = recovered.Task.LeaseOwner
		}
		if _, err := server.store.RecordWorkerEventReceipt(
			ctx, recovered.Request.RequestKey, step.Event.ID, receiptWorkerID, eventDigest,
		); err != nil {
			return fmt.Errorf("record recovered worker event receipt %q: %w", step.Event.ID, err)
		}
		seenEventIDs = append(seenEventIDs, step.Event.ID)
		if step.Done {
			return nil
		}
	}
	if current != completion.StateAdmitted && current != completion.StateReconciled && current != completion.StateAwaitingHuman {
		return fmt.Errorf(
			"%w: task state %q has no durable terminal response step",
			storeapi.ErrUnrecoverableRequest, current,
		)
	}
	latestTask, err := server.store.GetTask(ctx, recovered.Task.TaskKey)
	if err != nil {
		return err
	}
	if latestTask.State != current {
		return fmt.Errorf("%w: recovered state changed concurrently", storeapi.ErrUnrecoverableRequest)
	}
	recovered.Task = latestTask
	preferredWorker := latestTask.LeaseOwner
	if !completion.IsStableKey(preferredWorker) {
		return fmt.Errorf("%w: task state %q has no valid worker owner", storeapi.ErrUnrecoverableRequest, current)
	}
	assignment, err := server.assignmentFrom(recovered.Task, recovered.Request)
	if err != nil {
		return err
	}
	deliveries, err := server.hub.Restore(assignment, hub.RestoreOptions{
		WorkerID: preferredWorker, Accepted: current == completion.StateAwaitingHuman,
		SeenEventIDs: seenEventIDs,
	})
	if err != nil {
		return err
	}
	recovered.Task.State = current
	server.startBackground(func() {
		server.consumeSession(encoder, recovered.Task, recovered.Request, deliveries, nil)
	})
	return nil
}

func (server *Server) assignmentFrom(task storeapi.Task, request storeapi.Request) (completion.Assignment, error) {
	parsedTier, err := completion.ParseCapabilityTier(string(task.CapabilityTier))
	if err != nil || parsedTier != task.CapabilityTier {
		return completion.Assignment{}, fmt.Errorf("%w: invalid capability tier %q", storeapi.ErrUnrecoverableRequest, task.CapabilityTier)
	}
	identity := completion.RoutingIdentity{
		CallerID: task.CallerID, WorkspaceKey: task.WorkspaceKey,
		TaskID: task.TaskID, IdempotencyKey: request.IdempotencyKey,
		HarnessID: task.HarnessID, HarnessVersion: task.HarnessVersion,
		HarnessSessionID: task.HarnessSessionID,
		Root:             task.Root, ExecAllowed: task.ExecAllowed,
	}
	if err := identity.Validate(task.CapabilityTier); err != nil {
		return completion.Assignment{}, fmt.Errorf("%w: invalid routing identity: %v", storeapi.ErrUnrecoverableRequest, err)
	}
	var profile *adapter.Profile
	workerRoot := ""
	if task.CapabilityTier != completion.TierChat {
		resolved, ok := server.adapters.Resolve(task.HarnessID, task.HarnessVersion)
		if !ok {
			return completion.Assignment{}, fmt.Errorf(
				"%w: no exact adapter for %s@%s",
				storeapi.ErrUnrecoverableRequest, task.HarnessID, task.HarnessVersion,
			)
		}
		profile = &resolved
		workerRoot = assignmentWorkspaceRoot(task.CapabilityTier, task.Root, profile)
	}
	assignment := completion.Assignment{
		CallerID: task.CallerID, WorkspaceKey: task.WorkspaceKey,
		TaskID: task.TaskID, IdempotencyKey: request.IdempotencyKey,
		LeaseOwner: task.LeaseOwner, CapabilityTier: task.CapabilityTier,
		HarnessID: task.HarnessID, HarnessVersion: task.HarnessVersion,
		HarnessSessionID: task.HarnessSessionID,
		Root:             workerRoot, ExecAllowed: task.ExecAllowed, Adapter: profile,
		Request: request.CanonicalRequest,
	}
	if err := workerproto.ValidateEnvelopeSize(
		workerproto.MessageAssignment, assignment, server.config.MaxWorkerMessageBytes,
	); err != nil {
		return completion.Assignment{}, fmt.Errorf("%w: assignment exceeds worker wire limit: %v", storeapi.ErrUnrecoverableRequest, err)
	}
	return assignment, nil
}

func (server *Server) codecForDialect(value canonical.Dialect) (dialect.Codec, bool) {
	for _, codec := range server.codecs {
		if codec.Dialect() == value {
			return codec, true
		}
	}
	return nil, false
}

func (server *Server) runtimeContext() context.Context {
	server.runMu.RLock()
	defer server.runMu.RUnlock()
	if server.runContext == nil {
		return context.Background()
	}
	return server.runContext
}

func (server *Server) startBackground(run func()) {
	server.runWait.Add(1)
	go func() {
		defer server.runWait.Done()
		run()
	}()
}

// Wait blocks until all completion sessions started by this server have
// stopped. Callers must first stop accepting HTTP traffic and cancel the
// context passed to Recover; this keeps SQLite open until consumer cleanup and
// audit writes have finished.
func (server *Server) Wait() {
	server.runWait.Wait()
}

type streamReplayCursor struct {
	flush    func() error
	send     func(...[]byte) error
	sequence int64
	started  bool
}

type streamResponseWriter struct {
	response   http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
}

func newStreamResponseWriter(response http.ResponseWriter, timeout time.Duration) streamResponseWriter {
	return streamResponseWriter{
		response: response, controller: http.NewResponseController(response), timeout: timeout,
	}
}

func (writer streamResponseWriter) flush() error {
	return writer.controller.Flush()
}

// writeAndFlush bounds one transport write without imposing an absolute
// deadline on the stream. A caller may legitimately wait for a Human for
// minutes; only a socket that has stopped consuming an available SSE frame is
// released. ResponseWriter implementations without deadline support retain
// the standard net/http behavior.
func (writer streamResponseWriter) writeAndFlush(chunks ...[]byte) (resultErr error) {
	deadlineSet := false
	if writer.timeout > 0 {
		err := writer.controller.SetWriteDeadline(time.Now().Add(writer.timeout))
		switch {
		case err == nil:
			deadlineSet = true
		case errors.Is(err, http.ErrNotSupported):
			// Test recorders and some embedding middleware cannot expose the
			// underlying connection deadline. Continue with their old behavior.
		default:
			return fmt.Errorf("set SSE write deadline: %w", err)
		}
	}
	if deadlineSet {
		defer func() {
			if err := writer.controller.SetWriteDeadline(time.Time{}); resultErr == nil && err != nil {
				resultErr = fmt.Errorf("clear SSE write deadline: %w", err)
			}
		}()
	}
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		written, err := writer.response.Write(chunk)
		if err != nil {
			return err
		}
		if written != len(chunk) {
			return io.ErrShortWrite
		}
	}
	return writer.flush()
}

func (cursor streamReplayCursor) writeAndFlush(
	response http.ResponseWriter,
	chunks ...[]byte,
) error {
	if cursor.send != nil {
		return cursor.send(chunks...)
	}
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		written, err := response.Write(chunk)
		if err != nil {
			return err
		}
		if written != len(chunk) {
			return io.ErrShortWrite
		}
	}
	if cursor.flush == nil {
		return errors.New("HTTP response writer does not support flushing")
	}
	return cursor.flush()
}

// abortUndecidedHTTPResponse is used only before any response header has been
// written. A normal handler return would make net/http synthesize an empty 200,
// which is observably different from the durable response chosen on recovery.
func abortUndecidedHTTPResponse() {
	panic(http.ErrAbortHandler)
}

func (server *Server) resolveResponseDecisionAfterError(
	lookup storeapi.BeginRequestResult,
) (storeapi.BeginRequestResult, bool) {
	read, ok := server.readDurableDecisionAfterError(lookup.Request.RequestKey)
	if !ok {
		return storeapi.BeginRequestResult{}, false
	}
	if !lookup.Request.CanonicalRequest.Stream &&
		(!read.ResponseComplete || len(read.Response.Body) == 0) {
		return storeapi.BeginRequestResult{}, false
	}
	lookup.Request.Response = read.Response
	lookup.Request.ResponseComplete = read.ResponseComplete
	return lookup, true
}

func (server *Server) replayResponse(
	response http.ResponseWriter,
	request *http.Request,
	lookup storeapi.BeginRequestResult,
) {
	decided, err := server.awaitResponseDecision(request.Context(), lookup)
	if err != nil {
		var ok bool
		decided, ok = server.resolveResponseDecisionAfterError(lookup)
		if !ok {
			abortUndecidedHTTPResponse()
			return
		}
	}
	if decided.Request.RecoveryQuarantined {
		if decided.Request.Response.StatusCode != http.StatusOK {
			server.writeResponseDecision(response, decided.Task, decided.Request)
			return
		}
		cursor, err := server.beginStreamingResponse(request.Context(), response, decided)
		if err != nil {
			if !cursor.started {
				abortUndecidedHTTPResponse()
			}
			return
		}
		server.continueStreamingResponse(response, request, decided, cursor)
		return
	}
	if !decided.Request.CanonicalRequest.Stream {
		server.writeResponseDecision(response, decided.Task, decided.Request)
		return
	}
	if decided.Request.Response.StatusCode != http.StatusOK {
		server.writeResponseDecision(response, decided.Task, decided.Request)
		return
	}
	cursor, err := server.beginStreamingResponse(request.Context(), response, decided)
	if err != nil {
		if errors.Is(err, storeapi.ErrReplayPayloadExpired) {
			codec, ok := server.codecForDialect(decided.Task.Dialect)
			if !ok {
				abortUndecidedHTTPResponse()
				return
			}
			server.writeAPIError(
				response, codec, http.StatusGone, "replay_payload_expired",
				"exact replay payload has expired; use a new idempotency key",
			)
			return
		}
		server.logger.Error("replay durable completion response", "caller_id", decided.Request.CallerID,
			"idempotency_key", decided.Request.IdempotencyKey, "error", err)
		if !cursor.started {
			abortUndecidedHTTPResponse()
		}
		return
	}
	server.continueStreamingResponse(response, request, decided, cursor)
}

// awaitResponseDecision prevents a concurrent duplicate from inferring 200
// merely because BeginRequest and a few stream frames are durable. The first
// handler atomically chooses either a terminal HTTP response or the 200/SSE
// boundary; all replays wait for and then follow that immutable choice.
func (server *Server) awaitResponseDecision(
	ctx context.Context,
	lookup storeapi.BeginRequestResult,
) (storeapi.BeginRequestResult, error) {
	ctx, cancelContext := server.responseReadContext(ctx)
	defer cancelContext()
	if lookup.Request.Response.StatusCode != 0 {
		return lookup, nil
	}
	for {
		wakeup, cancel := server.responses.subscribe(lookup.Request.RequestKey)
		read, err := server.store.ReadResponse(ctx, lookup.Request.RequestKey, math.MaxInt64)
		if err != nil {
			cancel()
			return storeapi.BeginRequestResult{}, err
		}
		if read.Response.StatusCode != 0 {
			cancel()
			lookup.Request.Response = read.Response
			lookup.Request.ResponseComplete = read.ResponseComplete
			return lookup, nil
		}
		if err := waitForResponseChange(ctx, wakeup); err != nil {
			cancel()
			return storeapi.BeginRequestResult{}, err
		}
		cancel()
	}
}

// beginStreamingResponse makes phase B externally observable before Enqueue:
// it writes 200 plus every durable start frame and flushes them synchronously.
func (server *Server) beginStreamingResponse(
	ctx context.Context,
	response http.ResponseWriter,
	lookup storeapi.BeginRequestResult,
) (streamReplayCursor, error) {
	if lookup.Request.Response.StatusCode != http.StatusOK {
		return streamReplayCursor{}, fmt.Errorf("stream response status is %d", lookup.Request.Response.StatusCode)
	}
	_, ok := response.(http.Flusher)
	if !ok {
		return streamReplayCursor{}, errors.New("HTTP response writer does not support flushing")
	}
	read, err := server.readResponseBeforeReplay(ctx, lookup.Request.RequestKey)
	if err != nil {
		return streamReplayCursor{}, err
	}
	if read.Response.StatusCode != http.StatusOK {
		return streamReplayCursor{}, fmt.Errorf(
			"stream response snapshot status is %d", read.Response.StatusCode,
		)
	}
	type preparedEvent struct {
		sequence int64
		wire     []byte
	}
	prepared := make([]preparedEvent, 0, len(read.Events))
	for _, event := range read.Events {
		wire, err := responseEventWireData(event)
		if err != nil {
			return streamReplayCursor{}, err
		}
		prepared = append(prepared, preparedEvent{sequence: event.Sequence, wire: wire})
	}
	setSSEHeaders(response)
	response.Header().Set(headerTaskID, lookup.Task.TaskID)
	response.Header().Set(headerIdempotencyKey, lookup.Request.IdempotencyKey)
	response.WriteHeader(http.StatusOK)
	streamWriter := newStreamResponseWriter(response, server.config.StreamWriteTimeout)
	cursor := streamReplayCursor{
		flush:   streamWriter.flush,
		send:    streamWriter.writeAndFlush,
		started: true,
	}
	frames := make([][]byte, 0, len(prepared))
	for _, event := range prepared {
		if len(event.wire) != 0 {
			frames = append(frames, event.wire)
		}
		cursor.sequence = event.sequence
	}
	if err := cursor.send(frames...); err != nil {
		return cursor, err
	}
	return cursor, nil
}

// readResponseBeforeReplay pins the response decision, retention tombstone,
// completion flag, and all currently durable events in one Store snapshot
// before making a 200 observable. A purge racing the earlier idempotency lookup
// therefore yields either an explicit 410 or a complete in-memory replay, never
// a truncated success.
func (server *Server) readResponseBeforeReplay(
	requestContext context.Context,
	requestKey storeapi.RequestKey,
) (storeapi.ResponseRead, error) {
	runtimeContext := server.runtimeContext()
	ctx, cancel := context.WithCancel(requestContext)
	stopRuntimeCancel := context.AfterFunc(runtimeContext, cancel)
	defer func() {
		stopRuntimeCancel()
		cancel()
	}()
	if runtimeContext.Err() != nil {
		cancel()
	}

	for {
		read, err := server.store.ReadResponse(ctx, requestKey, 0)
		if err == nil {
			if read.PayloadPruned {
				return storeapi.ResponseRead{}, storeapi.ErrReplayPayloadExpired
			}
			return read, nil
		}
		if ctx.Err() != nil {
			return storeapi.ResponseRead{}, ctx.Err()
		}
		retry := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			retry.Stop()
			return storeapi.ResponseRead{}, ctx.Err()
		case <-retry.C:
		}
	}
}

func (server *Server) continueStreamingResponse(
	response http.ResponseWriter,
	request *http.Request,
	lookup storeapi.BeginRequestResult,
	cursor streamReplayCursor,
) {
	ctx, cancelContext := server.responseReadContext(request.Context())
	defer cancelContext()
	for {
		wakeup, cancel := server.responses.subscribe(lookup.Request.RequestKey)
		read, err := server.store.ReadResponse(ctx, lookup.Request.RequestKey, cursor.sequence)
		if err != nil {
			cancel()
			return
		}
		for _, event := range read.Events {
			wire, err := responseEventWireData(event)
			if err != nil {
				cancel()
				return
			}
			if len(wire) != 0 {
				if cursor.writeAndFlush(response, wire) != nil {
					cancel()
					return
				}
			}
			cursor.sequence = event.Sequence
		}
		if read.ResponseComplete {
			cancel()
			return
		}
		if err := waitForResponseChange(ctx, wakeup); err != nil {
			cancel()
			return
		}
		cancel()
	}
}

func (server *Server) identity(
	request *http.Request,
	callerID string,
) (completion.RoutingIdentity, completion.CapabilityTier, error) {
	tier, err := completion.ParseCapabilityTier(request.Header.Get(headerCapabilityTier))
	if err != nil {
		return completion.RoutingIdentity{}, "", err
	}
	return completion.RoutingIdentity{
		CallerID: callerID, WorkspaceKey: request.Header.Get(headerWorkspaceKey),
		TaskID: request.Header.Get(headerTaskID), IdempotencyKey: request.Header.Get(headerIdempotencyKey),
		HarnessID: request.Header.Get(headerHarnessID), HarnessVersion: request.Header.Get(headerHarnessVersion),
		Root: request.Header.Get(headerWorkspaceRoot), ExecAllowed: strings.EqualFold(strings.TrimSpace(request.Header.Get(headerAllowExec)), "true"),
	}, tier, nil
}

func assignmentWorkspaceRoot(
	tier completion.CapabilityTier,
	callerRoot string,
	profile *adapter.Profile,
) string {
	if tier == completion.TierChat || profile == nil {
		return ""
	}
	if profile.PathStyle == adapter.PathAbsolute {
		return callerRoot
	}
	return "/workspace"
}

// isOpenCodeAuxiliaryRequest isolates provider-owned title/summary calls from
// the durable Workspace task selected by static OpenCode provider headers.
// The exact captured auxiliary shape has no tools. A request that declares
// even a non-builtin tool remains in the Workspace task: the Human may use any
// caller-declared native tool, while adapter-specific mirror features still
// fail closed unless their exact mapping is present.
func isOpenCodeAuxiliaryRequest(profile adapter.Profile, request canonical.Request) bool {
	return profile.HarnessID == adapter.OpenCodeID &&
		profile.HarnessVersion == adapter.OpenCodeVersion &&
		len(request.Tools) == 0
}

// validateCallerDeclaration treats every caller-id header value as an
// authenticated identity assertion. A caller may omit it on generic gateway
// requests, but it can never use the header to select another principal's
// correctness namespace.
func validateCallerDeclaration(header http.Header, principalCallerID string) (string, error) {
	declared := ""
	for _, raw := range header.Values(headerCallerID) {
		for _, candidate := range strings.Split(raw, ",") {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			if candidate != principalCallerID {
				return "", errors.New("declared caller_id does not match authenticated caller")
			}
			declared = candidate
		}
	}
	return declared, nil
}

func (server *Server) authenticate(request *http.Request) (auth.Principal, error) {
	return server.auth.AuthenticateRequest(request)
}

func (server *Server) persistFrames(ctx context.Context, key storeapi.RequestKey, frames [][]byte) error {
	for _, frame := range frames {
		if _, err := server.appendResponseEvent(ctx, key, responseEventWire, frame); err != nil {
			return err
		}
	}
	return nil
}

func responseEventWireData(event storeapi.ResponseEvent) ([]byte, error) {
	switch event.Kind {
	case responseEventStream:
		return nil, nil
	case responseEventWire:
		return event.Data, nil
	case responseEventStep:
		return nil, nil
	case responseEventApplied:
		var step persistedStep
		if err := json.Unmarshal(event.Data, &step); err != nil {
			return nil, fmt.Errorf("decode durable response step: %w", err)
		}
		return step.Wire, nil
	default:
		return nil, fmt.Errorf("unknown response event kind %q", event.Kind)
	}
}

func (server *Server) failBeforeStream(
	response http.ResponseWriter,
	codec dialect.Codec,
	task storeapi.Task,
	requestKey storeapi.RequestKey,
	audit *auditRun,
	code string,
	cause error,
) {
	decision := storeapi.ResponseDecision{
		StatusCode: http.StatusInternalServerError, ContentType: "application/json",
		Body: codec.AdmissionError(http.StatusInternalServerError, code, cause.Error()),
	}
	stored, err := server.failRequest(context.Background(), requestKey, task.State, decision)
	if err != nil {
		server.logger.Error("persist pre-stream failure", "caller_id", requestKey.CallerID,
			"idempotency_key", requestKey.IdempotencyKey, "error", err)
		server.writeAPIError(response, codec, http.StatusInternalServerError, "store_error", "failed to persist terminal response")
		return
	}
	server.completeAudit(context.Background(), audit, requestKey, code)
	server.writeResponseDecision(response, task, stored)
}

func (server *Server) writeResponseDecision(
	response http.ResponseWriter,
	task storeapi.Task,
	request storeapi.Request,
) {
	decision := request.Response
	if decision.ContentType != "" {
		response.Header().Set("Content-Type", decision.ContentType)
	}
	if decision.RetryAfter != "" {
		response.Header().Set("Retry-After", decision.RetryAfter)
	}
	response.Header().Set(headerTaskID, task.TaskID)
	response.Header().Set(headerIdempotencyKey, request.IdempotencyKey)
	response.WriteHeader(decision.StatusCode)
	_, _ = response.Write(decision.Body)
}

func (server *Server) writeAPIError(response http.ResponseWriter, codec dialect.Codec, status int, code, message string) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_, _ = response.Write(codec.AdmissionError(status, code, message))
}

func toolCallDigest(call completion.ToolCall) (string, error) {
	payload, err := json.Marshal(call)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func workerEventDigest(event completion.Event) (string, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("marshal worker event digest: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalToolResults(request canonical.Request) []storeapi.ToolResult {
	var results []storeapi.ToolResult
	for _, message := range request.Messages {
		for _, block := range message.Blocks {
			if block.Type != canonical.BlockToolResult {
				continue
			}
			payload, err := json.Marshal(block.Output)
			if err != nil {
				// Decode and canonical validation only produce JSON-compatible
				// values. Preserve an invalid marker so admission fails closed if
				// a future codec violates that contract.
				payload = nil
			}
			results = append(results, storeapi.ToolResult{
				ToolCallID: block.ToolCallID,
				Result:     payload,
				IsError:    block.IsError,
			})
		}
	}
	return results
}

func setSSEHeaders(response http.ResponseWriter) {
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-cache, no-transform")
	response.Header().Set("Connection", "keep-alive")
	response.Header().Set("X-Accel-Buffering", "no")
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
