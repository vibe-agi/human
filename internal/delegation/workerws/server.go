// Package workerws exposes the private WebSocket transport between humand's
// delegation authority and a human worker. It never performs workspace or git
// operations; all authoritative changes go through delegation.Store.
package workerws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/auth"
	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
)

// DefaultPath is intentionally distinct from the synchronous completion
// worker transport. The command packages may register it when delegation is
// wired without content-negotiating two protocols on one socket.
const DefaultPath = "/internal/v1/delegation/worker/ws"

type Config struct {
	ReadLimit      int64
	WriteTimeout   time.Duration
	SnapshotPoll   time.Duration
	OutboundBuffer int
	RemoteExec     bool
}

func (config Config) withDefaults() Config {
	if config.ReadLimit <= 0 {
		config.ReadLimit = 16 << 20
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.SnapshotPoll <= 0 {
		config.SnapshotPoll = 200 * time.Millisecond
	}
	if config.OutboundBuffer <= 0 {
		config.OutboundBuffer = 128
	}
	return config
}

// Authority is the complete server-side boundary used by this transport.
// *delegation.Store satisfies it. In particular, this interface contains no
// filesystem or git operation.
type Authority interface {
	ListRecoverableTasks(context.Context) ([]delegation.Task, error)
	LoadSnapshot(context.Context, string) (delegation.Snapshot, error)
	ExecuteWorkerCommand(context.Context, delegation.WorkerCommandReceipt, delegation.WorkerCommandApply) (delegation.WorkerCommandReceipt, bool, error)
}

type Server struct {
	config    Config
	auth      auth.Authenticator
	authority Authority
	commandMu sync.Mutex
}

func New(config Config, authenticator auth.Authenticator, authority Authority) (*Server, error) {
	if authenticator == nil || authority == nil {
		return nil, errors.New("delegation worker authenticator and authority are required")
	}
	return &Server{config: config.withDefaults(), auth: authenticator, authority: authority}, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	principal, err := server.authenticate(request)
	if err != nil || principal.Type != auth.PrincipalWorker || strings.TrimSpace(principal.SubjectID) == "" {
		http.Error(response, "worker token required", http.StatusUnauthorized)
		return
	}
	connection, err := websocket.Accept(response, request, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		return
	}
	defer connection.CloseNow()
	connection.SetReadLimit(server.config.ReadLimit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbound := make(chan workerproto.Envelope, server.config.OutboundBuffer)
	errCh := make(chan error, 2)
	var lastIncoming atomic.Uint64

	hello, _ := workerproto.NewEnvelope(workerproto.MessageHello, workerproto.Hello{WorkerID: principal.SubjectID})
	outbound <- hello
	go func() {
		server.writeLoop(ctx, connection, outbound, &lastIncoming, errCh)
		cancel()
	}()
	go func() {
		server.readLoop(ctx, connection, outbound, principal.SubjectID, &lastIncoming, errCh)
		cancel()
	}()

	seen := make(map[string]int64)
	if err := server.syncSnapshots(ctx, principal.SubjectID, seen, true, outbound); err != nil {
		_ = connection.Close(websocket.StatusInternalError, err.Error())
		return
	}
	ticker := time.NewTicker(server.config.SnapshotPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			if err != nil && !isNormalClose(err) {
				_ = connection.Close(websocket.StatusPolicyViolation, err.Error())
			}
			return
		case <-ticker.C:
			if err := server.syncSnapshots(ctx, principal.SubjectID, seen, false, outbound); err != nil {
				_ = connection.Close(websocket.StatusInternalError, err.Error())
				return
			}
		}
	}
}

func (server *Server) authenticate(request *http.Request) (auth.Principal, error) {
	authorization := request.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		return auth.Principal{}, auth.ErrUnauthorized
	}
	return server.auth.Authenticate(request.Context(), strings.TrimSpace(authorization[len("Bearer "):]))
}

func (server *Server) writeLoop(
	ctx context.Context,
	connection *websocket.Conn,
	outbound <-chan workerproto.Envelope,
	lastIncoming *atomic.Uint64,
	errCh chan<- error,
) {
	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			return
		case envelope, open := <-outbound:
			if !open {
				return
			}
			sequence++
			envelope.Version = workerproto.Version
			envelope.Seq = sequence
			envelope.Ack = lastIncoming.Load()
			writeCtx, cancel := context.WithTimeout(ctx, server.config.WriteTimeout)
			err := wsjson.Write(writeCtx, connection, envelope)
			cancel()
			if err != nil {
				report(errCh, err)
				return
			}
		}
	}
}

func (server *Server) readLoop(
	ctx context.Context,
	connection *websocket.Conn,
	outbound chan<- workerproto.Envelope,
	workerID string,
	lastIncoming *atomic.Uint64,
	errCh chan<- error,
) {
	for {
		var envelope workerproto.Envelope
		if err := wsjson.Read(ctx, connection, &envelope); err != nil {
			report(errCh, err)
			return
		}
		if err := envelope.Validate(); err != nil {
			report(errCh, err)
			return
		}
		last := lastIncoming.Load()
		if envelope.Seq <= last {
			ack, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
			if !enqueue(ctx, outbound, ack) {
				return
			}
			continue
		}
		if envelope.Seq != last+1 {
			report(errCh, errors.New("delegation worker message sequence gap"))
			return
		}
		lastIncoming.Store(envelope.Seq)
		switch envelope.Type {
		case workerproto.MessageAck:
			continue
		case workerproto.MessageCommand:
			var command workerproto.Command
			if err := json.Unmarshal(envelope.Payload, &command); err != nil {
				report(errCh, fmt.Errorf("decode delegation worker command: %w", err))
				return
			}
			if err := command.Validate(); err != nil {
				report(errCh, err)
				return
			}
			result, err := server.executeCommand(ctx, workerID, command)
			if err != nil {
				report(errCh, err)
				return
			}
			response, err := workerproto.NewEnvelope(workerproto.MessageCommandResult, result)
			if err != nil || !enqueue(ctx, outbound, response) {
				if err != nil {
					report(errCh, err)
				}
				return
			}
		default:
			report(errCh, errors.New("worker may only send ack or command messages"))
			return
		}
	}
}

func (server *Server) executeCommand(
	ctx context.Context,
	workerID string,
	command workerproto.Command,
) (workerproto.CommandResult, error) {
	payload, err := json.Marshal(command)
	if err != nil {
		return workerproto.CommandResult{}, err
	}
	digestBytes := sha256.Sum256(payload)
	digest := hex.EncodeToString(digestBytes[:])

	server.commandMu.Lock()
	defer server.commandMu.Unlock()
	receipt, replay, err := server.authority.ExecuteWorkerCommand(ctx, delegation.WorkerCommandReceipt{
		EventID: command.EventID, WorkerID: workerID, TaskID: command.TaskID,
		CommandDigest: digest,
	}, func(ctx context.Context, authority delegation.WorkerCommandAuthority) ([]byte, bool, error) {
		result := workerproto.CommandResult{EventID: command.EventID, Kind: command.Kind}
		if applyErr := server.applyCommandToAuthority(ctx, authority, workerID, command, &result); applyErr != nil {
			if !stableCommandError(applyErr) {
				return nil, false, applyErr
			}
			result.Error = protocolError(applyErr)
			encoded, encodeErr := json.Marshal(result)
			return encoded, false, encodeErr
		}
		encoded, encodeErr := json.Marshal(result)
		return encoded, true, encodeErr
	})
	if err != nil {
		if errors.Is(err, delegation.ErrIdempotencyConflict) {
			return workerproto.CommandResult{
				EventID: command.EventID, Kind: command.Kind, Error: protocolError(err),
			}, nil
		}
		return workerproto.CommandResult{}, err
	}
	var result workerproto.CommandResult
	if err := json.Unmarshal(receipt.Result, &result); err != nil {
		return workerproto.CommandResult{}, fmt.Errorf("decode delegation worker command receipt: %w", err)
	}
	result.Replay = replay
	return result, nil
}

func (server *Server) applyCommandToAuthority(
	ctx context.Context,
	authority delegation.WorkerCommandAuthority,
	workerID string,
	command workerproto.Command,
	result *workerproto.CommandResult,
) error {
	task, err := authority.GetTask(ctx, command.TaskID)
	if err != nil {
		return err
	}
	if command.Kind != workerproto.CommandAccept && command.Kind != workerproto.CommandReject &&
		task.WorkerID != workerID {
		return fmt.Errorf("%w: task %q belongs to worker %q", errWorkerOwnership, task.ID, task.WorkerID)
	}
	input := delegation.CommandInput{
		TaskID: command.TaskID, ExpectedRevision: command.ExpectedRevision,
		Data: append([]byte(nil), command.Data...),
	}
	switch command.Kind {
	case workerproto.CommandAccept:
		transition, err := authority.AcceptTask(ctx, delegation.AcceptTaskInput{
			CommandInput: input, WorkerID: workerID,
		})
		if err == nil {
			result.Transition = &transition
		}
		return err
	case workerproto.CommandReject:
		transition, err := authority.RejectTask(ctx, input)
		if err == nil {
			result.Transition = &transition
		}
		return err
	case workerproto.CommandDeliver:
		delivery, err := authority.DeliverTurn(ctx, delegation.DeliverTurnInput{
			CommandInput: input, ArtifactID: command.Delivery.ArtifactID,
			ArtifactMediaType: command.Delivery.ArtifactMediaType,
			ArtifactData:      append([]byte(nil), command.Delivery.ArtifactData...),
			ArtifactMetadata:  append([]byte(nil), command.Delivery.ArtifactMetadata...),
		})
		if err == nil {
			result.Delivery = &delivery
		}
		return err
	case workerproto.CommandExec:
		if !server.config.RemoteExec {
			return fmt.Errorf("%w: remote execution is disabled", delegation.ErrInvalidInput)
		}
		execResult, err := authority.RequestExec(ctx, delegation.RequestExecInput{
			CommandInput: input,
			WorkerID:     workerID,
			RequestID:    command.Exec.RequestID,
			Command:      command.Exec.Command,
			CWD:          command.Exec.CWD,
			TimeoutMS:    command.Exec.TimeoutMS,
			Reason:       command.Exec.Reason,
		})
		if err == nil {
			result.Exec = &execResult
		}
		return err
	case workerproto.CommandComplete:
		transition, err := authority.CompleteTask(ctx, input)
		if err == nil {
			result.Transition = &transition
		}
		return err
	case workerproto.CommandFail:
		transition, err := authority.FailTask(ctx, input)
		if err == nil {
			result.Transition = &transition
		}
		return err
	case workerproto.CommandConfirmRewind:
		transition, err := authority.ConfirmRewind(ctx, input)
		if err == nil {
			result.Transition = &transition
		}
		return err
	case workerproto.CommandRejectRewind:
		transition, err := authority.RejectRewind(ctx, input)
		if err == nil {
			result.Transition = &transition
		}
		return err
	default:
		return fmt.Errorf("%w: unsupported command %q", delegation.ErrInvalidInput, command.Kind)
	}
}

var errWorkerOwnership = errors.New("delegation worker ownership conflict")

func stableCommandError(err error) bool {
	return errors.Is(err, delegation.ErrNotFound) ||
		errors.Is(err, delegation.ErrAlreadyExists) ||
		errors.Is(err, delegation.ErrRevisionConflict) ||
		errors.Is(err, delegation.ErrIdempotencyConflict) ||
		errors.Is(err, delegation.ErrInvalidTransition) ||
		errors.Is(err, delegation.ErrInvalidRewind) ||
		errors.Is(err, delegation.ErrInvalidInput) ||
		errors.Is(err, errWorkerOwnership)
}

func protocolError(err error) *workerproto.Error {
	code := "internal"
	switch {
	case errors.Is(err, delegation.ErrNotFound):
		code = "not_found"
	case errors.Is(err, delegation.ErrAlreadyExists):
		code = "already_exists"
	case errors.Is(err, delegation.ErrRevisionConflict):
		code = "revision_conflict"
	case errors.Is(err, delegation.ErrIdempotencyConflict):
		code = "idempotency_conflict"
	case errors.Is(err, delegation.ErrInvalidTransition):
		code = "invalid_transition"
	case errors.Is(err, delegation.ErrInvalidRewind):
		code = "invalid_rewind"
	case errors.Is(err, delegation.ErrInvalidInput):
		code = "invalid_input"
	case errors.Is(err, errWorkerOwnership):
		code = "ownership_conflict"
	}
	return &workerproto.Error{Code: code, Message: err.Error()}
}

func (server *Server) syncSnapshots(
	ctx context.Context,
	workerID string,
	seen map[string]int64,
	initial bool,
	outbound chan<- workerproto.Envelope,
) error {
	tasks, err := server.authority.ListRecoverableTasks(ctx)
	if err != nil {
		return err
	}
	visible := make(map[string]struct{})
	for _, task := range tasks {
		if task.State != delegation.StateSubmitted && task.WorkerID != workerID {
			continue
		}
		visible[task.ID] = struct{}{}
		if !initial && seen[task.ID] == task.Revision {
			continue
		}
		snapshot, err := server.authority.LoadSnapshot(ctx, task.ID)
		if err != nil {
			return err
		}
		reason := snapshotReason(snapshot, initial)
		envelope, err := workerproto.NewEnvelope(workerproto.MessageSnapshot, workerproto.Snapshot{
			EventID: authorityEventID(task.ID, task.Revision, "snapshot"),
			Reason:  reason, Snapshot: snapshot,
		})
		if err != nil {
			return err
		}
		if !enqueue(ctx, outbound, envelope) {
			return ctx.Err()
		}
		seen[task.ID] = task.Revision
	}
	for taskID := range seen {
		if _, ok := visible[taskID]; ok {
			continue
		}
		envelope, err := workerproto.NewEnvelope(workerproto.MessageTaskRemoved, workerproto.TaskRemoved{
			EventID: authorityEventID(taskID, seen[taskID], "removed"), TaskID: taskID,
		})
		if err != nil {
			return err
		}
		if !enqueue(ctx, outbound, envelope) {
			return ctx.Err()
		}
		delete(seen, taskID)
	}
	return nil
}

func authorityEventID(taskID string, revision int64, kind string) string {
	digest := sha256.Sum256([]byte(taskID + "\x00" + strconv.FormatInt(revision, 10) + "\x00" + kind))
	return "dwe_" + hex.EncodeToString(digest[:])
}

func snapshotReason(snapshot delegation.Snapshot, initial bool) workerproto.SnapshotReason {
	if initial || len(snapshot.Events) == 0 {
		return workerproto.ReasonRecovery
	}
	last := snapshot.Events[len(snapshot.Events)-1]
	switch last.Kind {
	case delegation.EventTaskSubmitted:
		return workerproto.ReasonSubmitted
	case delegation.EventRewindRequested:
		return workerproto.ReasonRewind
	case delegation.EventExecRequested, delegation.EventExecCompleted,
		delegation.EventExecDenied, delegation.EventExecFailed:
		return workerproto.ReasonExec
	case delegation.EventMessageAppended, delegation.EventCallerReplied:
		if messageIntent(last.Data) == "interrupt" {
			return workerproto.ReasonInterrupt
		}
		return workerproto.ReasonMessage
	default:
		return workerproto.ReasonState
	}
}

func messageIntent(data []byte) string {
	var message struct {
		Metadata map[string]any `json:"metadata"`
	}
	if json.Unmarshal(data, &message) != nil {
		return ""
	}
	intent, _ := message.Metadata["intent"].(string)
	return strings.ToLower(strings.TrimSpace(intent))
}

func enqueue(ctx context.Context, outbound chan<- workerproto.Envelope, envelope workerproto.Envelope) bool {
	select {
	case outbound <- envelope:
		return true
	case <-ctx.Done():
		return false
	}
}

func report(channel chan<- error, err error) {
	select {
	case channel <- err:
	default:
	}
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
