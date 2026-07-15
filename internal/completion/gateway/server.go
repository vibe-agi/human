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
	ResponseID    string `json:"response_id"`
	Model         string `json:"model"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type persistedStep struct {
	Event         completion.Event `json:"event"`
	Wire          []byte           `json:"wire,omitempty"`
	Done          bool             `json:"done"`
	EncodedAtUnix int64            `json:"encoded_at_unix"`
}

type Config struct {
	Models            []string
	MaxBodyBytes      int64
	HeartbeatInterval time.Duration
	MaxPending        time.Duration
	RateLimit         ratelimit.Config   // zero values select the limiter defaults
	RateLimiter       *ratelimit.Limiter // when set, overrides RateLimit
	AuditPayload      bool
	AuditPayloadTTL   time.Duration
}

func (config Config) withDefaults() Config {
	if len(config.Models) == 0 {
		config.Models = []string{"human-expert"}
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = 16 << 20
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	if config.MaxPending <= 0 {
		config.MaxPending = 10 * time.Minute
	}
	if config.AuditPayloadTTL <= 0 {
		config.AuditPayloadTTL = 7 * 24 * time.Hour
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
		slog.Error("create audit metadata", "audit_id", id, "error", err)
		return nil
	}
	if server.config.AuditPayload {
		if _, err := server.audit.StoreAuditPayload(ctx, storeapi.AuditPayload{
			AuditID: id, Kind: "request", Data: bytes.Clone(requestPayload),
			CreatedAt: now, ExpiresAt: now.Add(server.config.AuditPayloadTTL),
		}); err != nil {
			slog.Error("store request audit payload", "audit_id", id, "error", err)
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
		slog.Error("complete audit metadata", "audit_id", audit.id, "error", err)
	}
	if !server.config.AuditPayload {
		return
	}
	events, err := server.store.ListResponseEvents(ctx, requestKey, 0)
	if err != nil {
		slog.Error("load response audit payload", "audit_id", audit.id, "error", err)
		return
	}
	var payload bytes.Buffer
	for _, event := range events {
		wire, err := responseEventWireData(event)
		if err != nil {
			slog.Error("decode response audit event", "audit_id", audit.id, "sequence", event.Sequence, "error", err)
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
		slog.Error("store response audit payload", "audit_id", audit.id, "error", err)
	}
}

func (*Server) health(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	_, _ = response.Write([]byte(`{"status":"ok"}`))
}

func (server *Server) models(response http.ResponseWriter, request *http.Request) {
	if _, err := server.authenticate(request); err != nil {
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
		server.writeAPIError(response, codec, http.StatusBadRequest, "invalid_request", "request body is too large or unreadable")
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

	identity, tier := server.identity(request, callerID)
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
		if !known || identity.Validate(tier) != nil {
			tier = completion.TierChat
		} else {
			resolvedProfile = &profile
		}
	}
	if tier == completion.TierChat {
		canonicalRequest.Tools = nil
		identity.WorkspaceKey = ""
		identity.HarnessID = ""
		identity.HarnessVersion = ""
		identity.Root = ""
		identity.ExecAllowed = false
	}

	idempotencyKey := identity.IdempotencyKey
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
		server.replayResponse(response, request, lookup, digest)
		return
	case errors.Is(err, storeapi.ErrIdempotencyConflict):
		server.writeAPIError(response, codec, http.StatusConflict, "idempotency_conflict", "idempotency key was already used for a different request")
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
	if existing, getErr := server.store.GetTask(request.Context(), taskKey); getErr == nil {
		preferredWorker = existing.LeaseOwner
	} else if !errors.Is(getErr, storeapi.ErrNotFound) {
		server.writeAPIError(response, codec, http.StatusInternalServerError, "store_error", "failed to inspect task state")
		return
	}
	reservation, err := server.hub.Reserve(preferredWorker)
	if err != nil {
		status := codec.OverloadedStatus()
		response.Header().Set("Retry-After", "5")
		server.writeAPIError(response, codec, status, "overloaded", err.Error())
		return
	}
	reservationTransferred := false
	defer func() {
		if !reservationTransferred {
			reservation.Release()
		}
	}()

	begin, err := server.store.BeginRequest(request.Context(), storeapi.BeginRequestInput{
		Task: storeapi.Task{
			TaskKey: taskKey, WorkspaceKey: identity.WorkspaceKey,
			CapabilityTier: tier, Dialect: codec.Dialect(),
			HarnessID: identity.HarnessID, HarnessVersion: identity.HarnessVersion,
			Root: identity.Root, ExecAllowed: identity.ExecAllowed,
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
		case errors.Is(err, storeapi.ErrTaskConflict), errors.Is(err, storeapi.ErrTaskNotReady),
			errors.Is(err, storeapi.ErrToolResultsMissing), errors.Is(err, storeapi.ErrToolCallConflict):
			server.writeAPIError(response, codec, http.StatusConflict, "task_reconciliation_conflict", err.Error())
		default:
			server.writeAPIError(response, codec, http.StatusInternalServerError, "store_error", "failed to admit request")
		}
		return
	}
	if begin.Replay {
		server.replayResponse(response, request, begin, digest)
		return
	}
	audit := server.beginAudit(request.Context(), principal, begin.Task, requestKey, body)

	responseID, err := canonical.NewOpaqueID("chatcmpl_")
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "internal_error", errors.New("failed to allocate response id"))
		return
	}
	streamSeed := dialect.StreamSeed{CreatedAtUnix: time.Now().UTC().Unix()}
	metadata, err := json.Marshal(streamMetadata{
		ResponseID: responseID, Model: canonicalRequest.Model,
		CreatedAtUnix: streamSeed.CreatedAtUnix,
	})
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "internal_error", err)
		return
	}
	streamCheckpoint, err := server.store.AppendResponseEvent(request.Context(), requestKey, responseEventStream, metadata)
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "store_error", err)
		return
	}
	stream := codec.NewStream(responseID, canonicalRequest.Model, streamSeed)
	startFrames, err := stream.Start()
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "stream_start_failed", err)
		return
	}
	startSequence := streamCheckpoint.Sequence
	for _, frame := range startFrames {
		persisted, err := server.store.AppendResponseEvent(request.Context(), requestKey, responseEventWire, frame)
		if err != nil {
			server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "store_error", err)
			return
		}
		startSequence = persisted.Sequence
	}

	if _, ok := response.(http.Flusher); !ok {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "streaming_unsupported", errors.New("HTTP response writer does not support flushing"))
		return
	}
	begin.Request, err = server.store.BeginResponse(request.Context(), requestKey)
	if err != nil {
		server.failBeforeStream(response, codec, begin.Task, requestKey, audit, "store_error", fmt.Errorf("commit streaming response boundary: %w", err))
		return
	}
	virtualRoot := ""
	if tier != completion.TierChat {
		virtualRoot = "/workspace"
	}
	assignment := completion.Assignment{
		CallerID: callerID, WorkspaceKey: identity.WorkspaceKey,
		TaskID: taskID, IdempotencyKey: idempotencyKey,
		CapabilityTier: tier, HarnessID: identity.HarnessID, HarnessVersion: identity.HarnessVersion,
		Root: virtualRoot, ExecAllowed: identity.ExecAllowed, Adapter: resolvedProfile,
		Request: canonicalRequest,
	}

	// Once 200 is durable, the daemon—not this HTTP request—owns admission.
	// The gate preserves flush-before-Enqueue while the client is present. If
	// the client disappears, opening the gate abandons only that socket; the
	// durable request still progresses and can be replayed by an online retry.
	dispatchReady := make(chan struct{})
	reservationTransferred = true
	go server.dispatchCommittedRequest(
		server.runtimeContext(), dispatchReady, reservation, assignment,
		stream, begin.Task, begin.Request, audit,
	)

	cursor, err := server.beginFreshStreamingResponse(response, request, begin, startFrames, startSequence)
	close(dispatchReady)
	if err != nil {
		if request.Context().Err() == nil && server.runtimeContext().Err() == nil {
			slog.Error("start durable completion response", "caller_id", callerID, "idempotency_key", idempotencyKey, "error", err)
		}
		return
	}
	server.continueStreamingResponse(response, request, begin, digest, cursor)
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
	flusher, ok := response.(http.Flusher)
	if !ok {
		return streamReplayCursor{}, errors.New("HTTP response writer does not support flushing")
	}
	setSSEHeaders(response)
	response.Header().Set(headerTaskID, lookup.Task.TaskID)
	response.Header().Set(headerIdempotencyKey, lookup.Request.IdempotencyKey)
	response.WriteHeader(http.StatusOK)
	cursor := streamReplayCursor{flusher: flusher, sequence: startSequence}
	for _, frame := range startFrames {
		if len(frame) == 0 {
			continue
		}
		if _, err := response.Write(frame); err != nil {
			return cursor, err
		}
	}
	flusher.Flush()
	return cursor, nil
}

func (server *Server) dispatchCommittedRequest(
	ctx context.Context,
	ready <-chan struct{},
	reservation *hub.Reservation,
	assignment completion.Assignment,
	stream dialect.Stream,
	task storeapi.Task,
	request storeapi.Request,
	audit *auditRun,
) {
	defer reservation.Release()
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
			Type: completion.EventUnavailable, ErrorCode: "worker_unavailable", Error: err.Error(),
		}
		pendingSteps := make(map[string]persistedStep)
		_, done, persistErr := server.persistAndApplyEventWithRetry(
			ctx, stream, task, request.CanonicalRequest,
			request.RequestKey, task.State, pendingSteps, event,
		)
		if persistErr != nil {
			slog.Error("persist post-admission worker loss", "caller_id", request.CallerID,
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
	stream dialect.Stream,
	task storeapi.Task,
	request storeapi.Request,
	events <-chan *hub.Delivery,
	audit *auditRun,
) {
	ctx := server.runtimeContext()
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
			if _, err := server.store.AppendResponseEvent(ctx, request.RequestKey, responseEventWire, stream.Heartbeat()); err != nil {
				slog.Error("persist completion heartbeat", "caller_id", request.CallerID, "idempotency_key", request.IdempotencyKey, "error", err)
				continue
			}
		case <-expiry.C:
			event := completion.Event{
				Type: completion.EventExpired, ErrorCode: "human_timeout",
				Error: "human response exceeded max_pending",
			}
			next, done, err := server.persistAndApplyEventWithRetry(
				ctx, stream, task, request.CanonicalRequest, request.RequestKey, current, pendingSteps, event,
			)
			if err != nil {
				slog.Error("expire completion session", "caller_id", request.CallerID, "idempotency_key", request.IdempotencyKey, "error", err)
				return
			}
			current = next
			_ = server.hub.Abort(request.CallerID, request.IdempotencyKey)
			if done {
				server.completeAudit(context.Background(), audit, request.RequestKey, event.ErrorCode)
			}
			return
		case delivery := <-events:
			event := delivery.Event
			next, done, err := server.persistAndApplyEventWithRetry(
				ctx, stream, task, request.CanonicalRequest, request.RequestKey, current, pendingSteps, event,
			)
			delivery.Commit(err)
			if err != nil {
				slog.Error("process completion event", "caller_id", request.CallerID, "idempotency_key", request.IdempotencyKey, "event_id", event.ID, "error", err)
				continue
			}
			current = next
			if event.Type == completion.EventAccepted && audit != nil && audit.acceptedAt.IsZero() {
				audit.acceptedAt = time.Now().UTC()
			}
			if done {
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
	stream dialect.Stream,
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
	stream dialect.Stream,
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
				return current, false, err
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
		if _, appendErr := server.store.AppendWorkerResponseEvent(
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
		if _, appendErr := server.store.AppendWorkerResponseEvent(
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
		if completeErr := server.store.CompleteRequest(ctx, requestKey); completeErr != nil {
			requestDigest, digestErr := request.Digest()
			if digestErr != nil {
				return next, false, digestErr
			}
			lookup, lookupErr := server.store.LookupRequest(ctx, requestKey, requestDigest)
			if lookupErr != nil || !lookup.Request.ResponseComplete {
				if lookupErr != nil {
					return next, false, workerEventStageStoreError("verify durable response completion", lookupErr)
				}
				return next, false, workerEventStageStoreError("complete durable response", completeErr)
			}
		}
	}
	if _, receiptErr := server.store.RecordWorkerEventReceipt(ctx, requestKey, event.ID, eventDigest); receiptErr != nil {
		if _, lookupErr := server.store.LookupWorkerEventReceipt(
			ctx, requestKey, event.ID, eventDigest,
		); lookupErr != nil {
			return next, false, workerEventStageStoreError("record worker event receipt", receiptErr)
		}
	}
	delete(pendingSteps, event.ID)
	return next, stages.step.Done, nil
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
	events, err := server.store.ListResponseEvents(ctx, requestKey, 0)
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
			if current == completion.StateAwaitingCaller {
				break
			}
			if current != completion.StateResponded {
				return current, unrecoverableWorkerEvent("clarification cannot resume from state %q", current)
			}
			if err := transition(completion.StateAwaitingCaller, ""); err != nil {
				return current, err
			}
		case completion.EventToolCalls:
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
	if task.CapabilityTier == completion.TierChat {
		return errors.New("Chat-tier tasks cannot dispatch tools")
	}
	profile, ok := server.adapters.Resolve(task.HarnessID, task.HarnessVersion)
	if !ok {
		return errors.New("task has no exact harness adapter")
	}
	declared := make(map[string]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		declared[tool.Name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(calls))
	for _, call := range calls {
		if strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Name) == "" {
			return errors.New("tool calls require stable ids and names")
		}
		if _, duplicate := seen[call.ID]; duplicate {
			return fmt.Errorf("duplicate tool call id %q", call.ID)
		}
		seen[call.ID] = struct{}{}
		if _, ok := declared[call.Name]; !ok {
			return fmt.Errorf("tool %q was not declared by the caller", call.Name)
		}
		if !profile.AllowsTool(call.Name) {
			return fmt.Errorf("tool %q is not allowed by adapter %s", call.Name, profile.Key())
		}
		if profile.IsExecTool(call.Name) && !task.ExecAllowed {
			return fmt.Errorf("exec tool %q is disabled by default", call.Name)
		}
	}
	if len(calls) == 0 {
		return errors.New("tool_calls response contains no calls")
	}
	return nil
}

// Recover reconstructs every durable, incomplete completion session before
// humand starts accepting traffic. A record that predates the durable stream
// metadata/step format is rejected explicitly instead of being redispatched
// with a fabricated or mismatched response stream.
func (server *Server) Recover(ctx context.Context) error {
	if ctx == nil {
		return errors.New("recovery context is required")
	}
	server.runMu.Lock()
	server.runContext = ctx
	server.runMu.Unlock()

	requests, err := server.store.ListRecoverableRequests(ctx)
	if err != nil {
		return fmt.Errorf("list recoverable completion requests: %w", err)
	}
	for _, request := range requests {
		if err := server.recoverRequest(ctx, request); err != nil {
			return fmt.Errorf(
				"recover completion request %s/%s: %w",
				request.Request.CallerID, request.Request.IdempotencyKey, err,
			)
		}
	}
	return nil
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
	hasStartWire := false
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
			hasStartWire = true
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
			CreatedAtUnix: time.Now().UTC().Unix(),
		}
		payload, err := json.Marshal(created)
		if err != nil {
			return err
		}
		if _, err := server.store.AppendResponseEvent(ctx, recovered.Request.RequestKey, responseEventStream, payload); err != nil {
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
	stream := codec.NewStream(metadata.ResponseID, metadata.Model, dialect.StreamSeed{
		CreatedAtUnix: metadata.CreatedAtUnix,
	})
	startFrames, err := stream.Start()
	if err != nil {
		return fmt.Errorf("restore stream start: %w", err)
	}
	if !hasStartWire {
		if len(steps) != 0 || len(appliedSteps) != 0 ||
			recovered.Task.State != completion.StateAdmitted && recovered.Task.State != completion.StateReconciled {
			return fmt.Errorf("%w: stream checkpoint has no start frame", storeapi.ErrUnrecoverableRequest)
		}
		if err := server.persistFrames(ctx, recovered.Request.RequestKey, startFrames); err != nil {
			return fmt.Errorf("create recovery stream start: %w", err)
		}
	}
	if recovered.Request.Response.StatusCode == 0 {
		decided, err := server.store.BeginResponse(ctx, recovered.Request.RequestKey)
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
		frames, done, err := stream.Encode(step.Event, dialect.EventSeed{
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
			if _, err := server.store.AppendWorkerResponseEvent(
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
			if err := server.store.CompleteRequest(ctx, recovered.Request.RequestKey); err != nil {
				return err
			}
		}
		if _, err := server.store.RecordWorkerEventReceipt(
			ctx, recovered.Request.RequestKey, step.Event.ID, eventDigest,
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
	if current == completion.StateAdmitted {
		if preferredWorker != "" {
			return fmt.Errorf("%w: admitted task unexpectedly has a lease owner", storeapi.ErrUnrecoverableRequest)
		}
	} else if preferredWorker == "" {
		return fmt.Errorf("%w: sticky task state %q has no lease owner", storeapi.ErrUnrecoverableRequest, current)
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
	go server.consumeSession(stream, recovered.Task, recovered.Request, deliveries, nil)
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
		Root: task.Root, ExecAllowed: task.ExecAllowed,
	}
	if err := identity.Validate(task.CapabilityTier); err != nil {
		return completion.Assignment{}, fmt.Errorf("%w: invalid routing identity: %v", storeapi.ErrUnrecoverableRequest, err)
	}
	var profile *adapter.Profile
	virtualRoot := ""
	if task.CapabilityTier != completion.TierChat {
		resolved, ok := server.adapters.Resolve(task.HarnessID, task.HarnessVersion)
		if !ok {
			return completion.Assignment{}, fmt.Errorf(
				"%w: no exact adapter for %s@%s",
				storeapi.ErrUnrecoverableRequest, task.HarnessID, task.HarnessVersion,
			)
		}
		profile = &resolved
		virtualRoot = "/workspace"
	}
	return completion.Assignment{
		CallerID: task.CallerID, WorkspaceKey: task.WorkspaceKey,
		TaskID: task.TaskID, IdempotencyKey: request.IdempotencyKey,
		LeaseOwner: task.LeaseOwner, CapabilityTier: task.CapabilityTier,
		HarnessID: task.HarnessID, HarnessVersion: task.HarnessVersion,
		Root: virtualRoot, ExecAllowed: task.ExecAllowed, Adapter: profile,
		Request: request.CanonicalRequest,
	}, nil
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
	return server.runContext
}

type streamReplayCursor struct {
	flusher  http.Flusher
	sequence int64
}

func (server *Server) replayResponse(
	response http.ResponseWriter,
	request *http.Request,
	lookup storeapi.BeginRequestResult,
	digest string,
) {
	decided, err := server.awaitResponseDecision(request.Context(), lookup, digest)
	if err != nil {
		return
	}
	if decided.Request.Response.StatusCode != http.StatusOK {
		server.writeResponseDecision(response, decided.Task, decided.Request)
		return
	}
	cursor, err := server.beginStreamingResponse(request.Context(), response, decided)
	if err != nil {
		slog.Error("replay durable completion response", "caller_id", decided.Request.CallerID,
			"idempotency_key", decided.Request.IdempotencyKey, "error", err)
		return
	}
	server.continueStreamingResponse(response, request, decided, digest, cursor)
}

// awaitResponseDecision prevents a concurrent duplicate from inferring 200
// merely because BeginRequest and a few stream frames are durable. The first
// handler atomically chooses either a terminal HTTP response or the 200/SSE
// boundary; all replays wait for and then follow that immutable choice.
func (server *Server) awaitResponseDecision(
	ctx context.Context,
	lookup storeapi.BeginRequestResult,
	digest string,
) (storeapi.BeginRequestResult, error) {
	if lookup.Request.Response.StatusCode != 0 {
		return lookup, nil
	}
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return storeapi.BeginRequestResult{}, ctx.Err()
		case <-poll.C:
		}
		refreshed, err := server.store.LookupRequest(ctx, lookup.Request.RequestKey, digest)
		if err != nil {
			return storeapi.BeginRequestResult{}, err
		}
		if refreshed.Request.Response.StatusCode != 0 {
			return refreshed, nil
		}
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
	flusher, ok := response.(http.Flusher)
	if !ok {
		return streamReplayCursor{}, errors.New("HTTP response writer does not support flushing")
	}
	events, err := server.listResponseEventsBeforeReplay(ctx, lookup.Request.RequestKey)
	if err != nil {
		return streamReplayCursor{}, err
	}
	setSSEHeaders(response)
	response.Header().Set(headerTaskID, lookup.Task.TaskID)
	response.Header().Set(headerIdempotencyKey, lookup.Request.IdempotencyKey)
	response.WriteHeader(http.StatusOK)
	cursor := streamReplayCursor{flusher: flusher}
	for _, event := range events {
		wire, err := responseEventWireData(event)
		if err != nil {
			return cursor, err
		}
		if len(wire) != 0 {
			_, _ = response.Write(wire)
		}
		cursor.sequence = event.Sequence
	}
	flusher.Flush()
	return cursor, nil
}

// listResponseEventsBeforeReplay retries the only store read between a durable
// 200 decision and the first byte of an idempotent replay. Returning a transient
// store failure here would otherwise make net/http synthesize an empty 200. Once
// the SSE response is observable, continueStreamingResponse deliberately keeps
// its disconnect-and-replay behavior instead of retrying reads on the socket.
func (server *Server) listResponseEventsBeforeReplay(
	requestContext context.Context,
	requestKey storeapi.RequestKey,
) ([]storeapi.ResponseEvent, error) {
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
		events, err := server.store.ListResponseEvents(ctx, requestKey, 0)
		if err == nil {
			return events, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		retry := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			retry.Stop()
			return nil, ctx.Err()
		case <-retry.C:
		}
	}
}

func (server *Server) continueStreamingResponse(
	response http.ResponseWriter,
	request *http.Request,
	lookup storeapi.BeginRequestResult,
	digest string,
	cursor streamReplayCursor,
) {
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		events, err := server.store.ListResponseEvents(request.Context(), lookup.Request.RequestKey, cursor.sequence)
		if err != nil {
			return
		}
		for _, event := range events {
			wire, err := responseEventWireData(event)
			if err != nil {
				return
			}
			if len(wire) != 0 {
				_, _ = response.Write(wire)
				cursor.flusher.Flush()
			}
			cursor.sequence = event.Sequence
		}
		refreshed, err := server.store.LookupRequest(request.Context(), lookup.Request.RequestKey, digest)
		if err != nil {
			return
		}
		if refreshed.Request.ResponseComplete {
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-poll.C:
		}
	}
}

func (server *Server) identity(request *http.Request, callerID string) (completion.RoutingIdentity, completion.CapabilityTier) {
	tier, err := completion.ParseCapabilityTier(request.Header.Get(headerCapabilityTier))
	if err != nil {
		tier = completion.TierChat
	}
	return completion.RoutingIdentity{
		CallerID: callerID, WorkspaceKey: request.Header.Get(headerWorkspaceKey),
		TaskID: request.Header.Get(headerTaskID), IdempotencyKey: request.Header.Get(headerIdempotencyKey),
		HarnessID: request.Header.Get(headerHarnessID), HarnessVersion: request.Header.Get(headerHarnessVersion),
		Root: request.Header.Get(headerWorkspaceRoot), ExecAllowed: strings.EqualFold(strings.TrimSpace(request.Header.Get(headerAllowExec)), "true"),
	}, tier
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
	secret := strings.TrimSpace(request.Header.Get("X-Api-Key"))
	if authorization := request.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		secret = strings.TrimSpace(authorization[len("Bearer "):])
	}
	if secret == "" {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return server.auth.Authenticate(request.Context(), secret)
}

func (server *Server) persistFrames(ctx context.Context, key storeapi.RequestKey, frames [][]byte) error {
	for _, frame := range frames {
		if _, err := server.store.AppendResponseEvent(ctx, key, responseEventWire, frame); err != nil {
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
	stored, err := server.store.FailRequest(context.Background(), requestKey, task.State, decision)
	if err != nil {
		slog.Error("persist pre-stream failure", "caller_id", requestKey.CallerID,
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
