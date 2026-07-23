// Package web is the official browser UI for the Human worker.
//
// It is a stateless projection over a workerkit.Worker: every business rule,
// transcript, draft, and continuation lives in workerkit and its StateStore;
// the web layer renders snapshots and forwards commands. The host owns the
// listener and the worker daemon lifetime — browsers are disposable sessions
// that can open and close without losing any state.
//
// Authentication is a single opaque session token chosen by the host. It is
// intended for loopback use or behind a host-owned TLS reverse proxy; the
// worker's transport credentials never reach the browser.
package web

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/workerkit"
)

const sessionCookie = "human_web_session"

var (
	ErrInvalidConfig = errors.New("web: invalid configuration")
	ErrClosed        = errors.New("web: server is closed")
)

// Config composes one Server. Worker is required and borrowed: the host shuts
// the Server down before the Worker. SessionToken is required and compared in
// constant time; generate it per daemon start and hand it to the operator as
// a login URL (http://host/?token=...).
type Config struct {
	Worker       *workerkit.Worker
	SessionToken string
	// Heartbeat bounds SSE keep-alive comments. Zero selects 25 seconds.
	Heartbeat time.Duration
	// PlanProfiles selects the normalized Tasks/Plan panel from an exact harness
	// request. Nil installs OfficialPlanProfiles. A typed nil is rejected.
	PlanProfiles PlanProfileResolver
	// CommandProfiles selects the normalized command editor from an exact
	// harness request. Nil installs OfficialCommandProfiles. A typed nil is
	// rejected.
	CommandProfiles CommandProfileResolver
}

// Server is an http.Handler serving the embedded UI, the JSON command API,
// and the SSE state stream. Construct with New; Shutdown stops the broadcast
// pump and closes every SSE session.
type Server struct {
	worker    *workerkit.Worker
	tokenHash [sha256.Size]byte
	heartbeat time.Duration
	plan      PlanProfileResolver
	command   CommandProfileResolver

	mu          sync.Mutex
	closed      bool
	subscribers map[chan struct{}]struct{}

	pumpCtx    context.Context
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}

	mux *http.ServeMux
}

// New validates the configuration and starts the notification broadcast pump.
func New(config Config) (*Server, error) {
	if config.Worker == nil {
		return nil, fmt.Errorf("%w: Worker is required", ErrInvalidConfig)
	}
	if len(config.SessionToken) < 16 {
		return nil, fmt.Errorf("%w: SessionToken of at least 16 bytes is required", ErrInvalidConfig)
	}
	plan := config.PlanProfiles
	if plan == nil {
		plan = OfficialPlanProfiles()
	} else if nilPlanProfileResolver(plan) {
		return nil, fmt.Errorf("%w: PlanProfiles is typed nil", ErrInvalidConfig)
	}
	command := config.CommandProfiles
	if command == nil {
		command = OfficialCommandProfiles()
	} else if nilCommandProfileResolver(command) {
		return nil, fmt.Errorf("%w: CommandProfiles is typed nil", ErrInvalidConfig)
	}
	heartbeat := config.Heartbeat
	if heartbeat == 0 {
		heartbeat = 25 * time.Second
	}
	if heartbeat < time.Second {
		return nil, fmt.Errorf("%w: Heartbeat must be at least 1s", ErrInvalidConfig)
	}
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	server := &Server{
		worker: config.Worker, tokenHash: sha256.Sum256([]byte(config.SessionToken)), heartbeat: heartbeat,
		plan:        plan,
		command:     command,
		subscribers: make(map[chan struct{}]struct{}),
		pumpCtx:     pumpCtx, pumpCancel: pumpCancel, pumpDone: make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", server.handleIndex)
	mux.HandleFunc("GET /api/state", server.withAuth(server.handleState))
	mux.HandleFunc("GET /api/events", server.withAuth(server.handleEvents))
	mux.HandleFunc("POST /api/accept", server.withAuth(server.handleAccept))
	mux.HandleFunc("POST /api/reject", server.withAuth(server.handleReject))
	mux.HandleFunc("POST /api/reply", server.withAuth(server.conversationText(server.worker.Reply)))
	mux.HandleFunc("POST /api/clarify", server.withAuth(server.conversationText(server.worker.Clarify)))
	mux.HandleFunc("POST /api/final", server.withAuth(server.conversationText(server.worker.Final)))
	mux.HandleFunc("POST /api/draft", server.withAuth(server.conversationText(server.worker.SaveDraft)))
	mux.HandleFunc("POST /api/tool-calls", server.withAuth(server.handleToolCalls))
	mux.HandleFunc("POST /api/workspace", server.withAuth(server.handleWorkspace))
	mux.HandleFunc("POST /api/review/deliver", server.withAuth(server.handleDeliverChanges))
	mux.HandleFunc("POST /api/review/discard", server.withAuth(server.handleDiscardChanges))
	mux.HandleFunc("POST /api/alerts/dismiss", server.withAuth(server.handleDismissAlert))
	mux.HandleFunc("POST /api/abandon", server.withAuth(server.handleAbandon))
	server.mux = mux
	go server.pump()
	return server, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	securityHeaders(response)
	response.Header().Set("Cache-Control", "no-store")
	server.mux.ServeHTTP(response, request)
}

// Shutdown stops the broadcast pump and wakes every SSE session so their
// handlers can drain. It does not shut down the borrowed Worker.
func (server *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is required", ErrInvalidConfig)
	}
	server.mu.Lock()
	server.closed = true
	server.mu.Unlock()
	server.pumpCancel()
	select {
	case <-server.pumpDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pump fans the worker's coalescing notification signal out to every SSE
// subscriber; each subscriber channel is itself coalescing.
func (server *Server) pump() {
	defer close(server.pumpDone)
	for {
		select {
		case <-server.worker.Notifications():
			server.mu.Lock()
			for subscriber := range server.subscribers {
				select {
				case subscriber <- struct{}{}:
				default:
				}
			}
			server.mu.Unlock()
		case <-server.pumpCtx.Done():
			return
		}
	}
}

func (server *Server) subscribe() (chan struct{}, func(), error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.closed {
		return nil, nil, ErrClosed
	}
	subscriber := make(chan struct{}, 1)
	server.subscribers[subscriber] = struct{}{}
	return subscriber, func() {
		server.mu.Lock()
		delete(server.subscribers, subscriber)
		server.mu.Unlock()
	}, nil
}

func (server *Server) authenticated(request *http.Request) bool {
	if cookie, err := request.Cookie(sessionCookie); err == nil {
		if server.tokenMatches(cookie.Value) {
			return true
		}
	}
	header := request.Header.Get("Authorization")
	if token, found := strings.CutPrefix(header, "Bearer "); found {
		return server.tokenMatches(token)
	}
	return false
}

func (server *Server) tokenMatches(candidate string) bool {
	candidateHash := sha256.Sum256([]byte(candidate))
	return subtle.ConstantTimeCompare(candidateHash[:], server.tokenHash[:]) == 1
}

func (server *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		if !server.authenticated(request) {
			writeError(response, http.StatusUnauthorized, "unauthenticated", "web session is not authenticated")
			return
		}
		next(response, request)
	}
}

// handleIndex serves the embedded UI. A one-time ?token=... exchange sets the
// session cookie and strips the token from the address bar via redirect.
func (server *Server) handleIndex(response http.ResponseWriter, request *http.Request) {
	securityHeaders(response)
	if token := request.URL.Query().Get("token"); token != "" {
		if !server.tokenMatches(token) {
			writeError(response, http.StatusUnauthorized, "unauthenticated", "invalid session token")
			return
		}
		http.SetCookie(response, &http.Cookie{
			Name: sessionCookie, Value: token, Path: "/",
			HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: requestIsHTTPS(request),
		})
		http.Redirect(response, request, "/", http.StatusSeeOther)
		return
	}
	if !server.authenticated(request) {
		writeError(response, http.StatusUnauthorized, "unauthenticated",
			"open the login URL printed by the worker daemon")
		return
	}
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	response.Header().Set("Cache-Control", "no-store")
	_, _ = response.Write(indexHTML)
}

func (server *Server) handleState(response http.ResponseWriter, request *http.Request) {
	writeJSON(response, http.StatusOK, server.stateView(server.worker.Snapshot()))
}

func (server *Server) handleEvents(response http.ResponseWriter, request *http.Request) {
	flusher, supported := response.(http.Flusher)
	if !supported {
		writeError(response, http.StatusInternalServerError, "streaming_unsupported", "response writer cannot stream")
		return
	}
	subscriber, unsubscribe, err := server.subscribe()
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "closed", "web server is shutting down")
		return
	}
	defer unsubscribe()
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(http.StatusOK)
	heartbeat := time.NewTicker(server.heartbeat)
	defer heartbeat.Stop()
	if !server.writeStateEvent(response, flusher) {
		return
	}
	for {
		select {
		case <-subscriber:
			if !server.writeStateEvent(response, flusher) {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(response, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-request.Context().Done():
			return
		case <-server.pumpCtx.Done():
			return
		}
	}
}

func (server *Server) writeStateEvent(response http.ResponseWriter, flusher http.Flusher) bool {
	encoded, err := json.Marshal(server.stateView(server.worker.Snapshot()))
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(response, "event: state\ndata: %s\n\n", encoded); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

type deliveryRequest struct {
	Delivery string `json:"delivery"`
	Reason   string `json:"reason,omitempty"`
}

type conversationRequest struct {
	Caller string `json:"caller"`
	TaskID string `json:"task_id"`
	Text   string `json:"text"`
}

type toolCallsRequest struct {
	Caller string         `json:"caller"`
	TaskID string         `json:"task_id"`
	Calls  []llm.ToolCall `json:"calls"`
}

func (server *Server) handleAccept(response http.ResponseWriter, request *http.Request) {
	var body deliveryRequest
	if !decodeBody(response, request, &body) {
		return
	}
	key, err := server.worker.Accept(request.Context(), llm.WorkerDeliveryID(body.Delivery))
	if err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"key": key})
}

func (server *Server) handleReject(response http.ResponseWriter, request *http.Request) {
	var body deliveryRequest
	if !decodeBody(response, request, &body) {
		return
	}
	if err := server.worker.Reject(request.Context(), llm.WorkerDeliveryID(body.Delivery), body.Reason); err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"ok": true})
}

func (server *Server) conversationText(
	command func(context.Context, workerkit.ConversationKey, string) error,
) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		var body conversationRequest
		if !decodeBody(response, request, &body) {
			return
		}
		key := workerkit.ConversationKey{Caller: llm.CallerID(body.Caller), TaskID: llm.TaskID(body.TaskID)}
		if err := command(request.Context(), key, body.Text); err != nil {
			writeCommandError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{"ok": true})
	}
}

func (server *Server) handleToolCalls(response http.ResponseWriter, request *http.Request) {
	var body toolCallsRequest
	if !decodeBody(response, request, &body) {
		return
	}
	key := workerkit.ConversationKey{Caller: llm.CallerID(body.Caller), TaskID: llm.TaskID(body.TaskID)}
	if err := server.worker.SubmitToolCalls(request.Context(), key, body.Calls); err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"ok": true})
}

func (server *Server) handleWorkspace(response http.ResponseWriter, request *http.Request) {
	var body struct {
		Caller string `json:"caller"`
		TaskID string `json:"task_id"`
		Path   string `json:"path"`
	}
	if !decodeBody(response, request, &body) {
		return
	}
	key := workerkit.ConversationKey{Caller: llm.CallerID(body.Caller), TaskID: llm.TaskID(body.TaskID)}
	workspace, err := server.worker.SetHumanWorkspace(request.Context(), key, body.Path)
	if err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"workspace": workspace})
}

type reviewRequest struct {
	Caller    string   `json:"caller,omitempty"`
	TaskID    string   `json:"task_id,omitempty"`
	ChangeIDs []string `json:"change_ids"`
}

func (server *Server) handleDeliverChanges(response http.ResponseWriter, request *http.Request) {
	var body reviewRequest
	if !decodeBody(response, request, &body) {
		return
	}
	key := workerkit.ConversationKey{Caller: llm.CallerID(body.Caller), TaskID: llm.TaskID(body.TaskID)}
	if err := server.worker.DeliverChanges(request.Context(), key, body.ChangeIDs); err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"ok": true})
}

func (server *Server) handleDismissAlert(response http.ResponseWriter, request *http.Request) {
	var body struct {
		Seq uint64 `json:"seq"`
	}
	if !decodeBody(response, request, &body) {
		return
	}
	if err := server.worker.DismissAlertContext(request.Context(), body.Seq); err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"ok": true})
}

func (server *Server) handleDiscardChanges(response http.ResponseWriter, request *http.Request) {
	var body reviewRequest
	if !decodeBody(response, request, &body) {
		return
	}
	if err := server.worker.DiscardChanges(request.Context(), body.ChangeIDs); err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"ok": true})
}

// handleAbandon terminates a conversation stuck awaiting a caller continuation
// that will never arrive. It carries no text: abandonment sends no new wire
// event (see workerkit.Worker.Abandon).
func (server *Server) handleAbandon(response http.ResponseWriter, request *http.Request) {
	var body struct {
		Caller string `json:"caller"`
		TaskID string `json:"task_id"`
	}
	if !decodeBody(response, request, &body) {
		return
	}
	key := workerkit.ConversationKey{Caller: llm.CallerID(body.Caller), TaskID: llm.TaskID(body.TaskID)}
	if err := server.worker.Abandon(request.Context(), key); err != nil {
		writeCommandError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"ok": true})
}

func decodeBody(response http.ResponseWriter, request *http.Request, target any) bool {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_body", "request body is invalid")
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(response, http.StatusBadRequest, "invalid_body", "request body is invalid")
		return false
	}
	return true
}

// writeCommandError maps a domain error to a status and a fixed, generic
// message. It never forwards err.Error() — downstream wrapping (e.g. fsmirror
// wraps filesystem paths) must not reach the browser, matching the caller
// adapter's no-internal-detail convention.
func writeCommandError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, workerkit.ErrUnknownDelivery), errors.Is(err, workerkit.ErrUnknownConversation),
		errors.Is(err, workerkit.ErrUnknownChange):
		writeError(response, http.StatusNotFound, "not_found", "the referenced item does not exist")
	case errors.Is(err, workerkit.ErrConversationTerminal),
		errors.Is(err, workerkit.ErrConversationNotActive),
		errors.Is(err, workerkit.ErrConversationNotAbandonable),
		errors.Is(err, workerkit.ErrTooManyContinuations),
		errors.Is(err, workerkit.ErrNoMirror):
		writeError(response, http.StatusConflict, "conflict", "the command is not valid for the current state")
	case errors.Is(err, workerkit.ErrInvalidCommand):
		writeError(response, http.StatusBadRequest, "invalid_command", "the command is invalid")
	case errors.Is(err, workerkit.ErrClosed):
		writeError(response, http.StatusServiceUnavailable, "closed", "the worker is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(response, http.StatusRequestTimeout, "canceled", "request was canceled")
	default:
		writeError(response, http.StatusBadGateway, "transport_failed", "the command could not be delivered")
	}
}

// securityHeaders hardens the document response: no external resources, no
// framing, no referrer leak (the login URL carries the session token), and no
// content-type sniffing.
func securityHeaders(response http.ResponseWriter) {
	header := response.Header()
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Content-Security-Policy",
		"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "+
			"img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
}

// requestIsHTTPS reports whether the request reached the daemon over TLS,
// directly or through a host-owned reverse proxy, so the session cookie can be
// marked Secure without breaking the loopback plaintext default.
func requestIsHTTPS(request *http.Request) bool {
	if request.TLS != nil {
		return true
	}
	return request.Header.Get("X-Forwarded-Proto") == "https"
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]string{"error": code, "message": message})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

// stateView is the Human-facing wire projection of a workerkit snapshot. It
// deliberately does not embed the durable Assignment record; only logical
// scope and the Human host's selected working directory are exposed.
func (server *Server) stateView(state workerkit.State) map[string]any {
	inbox := make([]map[string]any, 0, len(state.Inbox))
	for _, item := range state.Inbox {
		inbox = append(inbox, map[string]any{
			"delivery":    string(item.Delivery),
			"caller":      string(item.Key.Caller),
			"task_id":     string(item.Key.TaskID),
			"tier":        string(item.Tier),
			"preview":     item.Preview,
			"tool_count":  item.ToolCount,
			"received_at": item.ReceivedAt,
		})
	}
	type conversationView struct {
		Key            workerkit.ConversationKey   `json:"key"`
		Phase          workerkit.Phase             `json:"phase"`
		Transcript     []workerkit.TranscriptEntry `json:"transcript"`
		ParkedCalls    []llm.ToolCall              `json:"parked_calls,omitempty"`
		Delivery       *workerkit.PendingDelivery  `json:"delivery,omitempty"`
		Draft          string                      `json:"draft,omitempty"`
		CallerGone     bool                        `json:"caller_gone,omitempty"`
		UpdatedAt      time.Time                   `json:"updated_at"`
		WorkspaceScope string                      `json:"workspace_scope,omitempty"`
		HumanWorkspace *workerkit.HumanWorkspace   `json:"human_workspace,omitempty"`
		PlanProfile    *PlanProfile                `json:"plan_profile,omitempty"`
		CommandProfile *CommandProfile             `json:"command_profile,omitempty"`
	}
	conversations := make([]conversationView, 0, len(state.Conversations))
	for _, conversation := range state.Conversations {
		assignment := conversation.Assignment.Assignment
		view := conversationView{
			Key: conversation.Key, Phase: conversation.Phase,
			Transcript: conversation.Transcript, ParkedCalls: conversation.ParkedCalls,
			Delivery: conversation.Delivery, Draft: conversation.Draft,
			CallerGone: conversation.CallerGone, UpdatedAt: conversation.UpdatedAt,
			WorkspaceScope: assignment.Task.WorkspaceKey, HumanWorkspace: conversation.HumanWorkspace,
		}
		if profile, ok := server.plan.ResolvePlanProfile(assignment.Task, assignment.Request); ok && profile.Validate() == nil {
			copy := profile
			view.PlanProfile = &copy
		}
		if profile, ok := server.command.ResolveCommandProfile(assignment.Task, assignment.Request); ok && profile.Validate() == nil {
			copy := profile
			view.CommandProfile = &copy
		}
		conversations = append(conversations, view)
	}
	view := map[string]any{"schema_version": 3, "inbox": inbox, "conversations": conversations}
	if state.Review != nil {
		view["review"] = state.Review
	}
	if len(state.Alerts) > 0 {
		view["alerts"] = state.Alerts
	}
	return view
}
