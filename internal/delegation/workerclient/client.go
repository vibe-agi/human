// Package workerclient implements the human-side delegation WebSocket client.
// It exposes cached authority snapshots to worker.Service and persists every
// command before sending it, so reconnects reuse the same event ID.
package workerclient

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/delegation"
	"github.com/vibe-agi/human/internal/delegation/workerproto"
)

var (
	errConnectionUnavailable = errors.New("delegation worker connection is unavailable")
	ErrClientClosed          = errors.New("delegation worker client is closed")
	ErrOwnershipConflict     = errors.New("delegation task belongs to a different worker")
)

type Config struct {
	URL              string
	Token            string
	OutboxPath       string
	WriteTimeout     time.Duration
	ReconnectInitial time.Duration
	ReconnectMaximum time.Duration
	UpdateBuffer     int
}

func (config Config) withDefaults() (Config, error) {
	if strings.TrimSpace(config.URL) == "" || strings.TrimSpace(config.Token) == "" {
		return Config{}, errors.New("delegation worker URL and token are required")
	}
	if strings.TrimSpace(config.OutboxPath) == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Config{}, fmt.Errorf("resolve delegation worker outbox home: %w", err)
		}
		config.OutboxPath = filepath.Join(home, ".human", "delegation-worker-outbox.db")
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.ReconnectInitial <= 0 {
		config.ReconnectInitial = 100 * time.Millisecond
	}
	if config.ReconnectMaximum <= 0 {
		config.ReconnectMaximum = 5 * time.Second
	}
	if config.UpdateBuffer <= 0 {
		config.UpdateBuffer = 128
	}
	return config, nil
}

type Update struct {
	Snapshot      *workerproto.Snapshot
	RemovedTaskID string
	Result        *workerproto.CommandResult
	Err           error
}

type Client struct {
	config Config
	outbox *durableOutbox

	writeMu    sync.Mutex
	connection *websocket.Conn
	clientSeq  uint64
	serverSeq  atomic.Uint64
	inflight   map[string]uint64

	cacheMu   sync.RWMutex
	snapshots map[string]delegation.Snapshot
	workerID  string
	ready     chan struct{}
	readyOnce sync.Once

	waiterMu sync.Mutex
	waiters  map[string][]chan workerproto.CommandResult

	updates  chan Update
	wake     chan struct{}
	cancel   context.CancelFunc
	closing  atomic.Bool
	close    sync.Once
	wait     sync.WaitGroup
	closeErr error
}

func Dial(ctx context.Context, config Config) (*Client, error) {
	resolved, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	outbox, err := openDurableOutbox(ctx, resolved.OutboxPath, resolved.URL, resolved.Token)
	if err != nil {
		return nil, err
	}
	connection, _, err := websocket.Dial(ctx, resolved.URL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + resolved.Token}},
	})
	if err != nil {
		outbox.Close()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	client := &Client{
		config: resolved, outbox: outbox, connection: connection,
		inflight: make(map[string]uint64), snapshots: make(map[string]delegation.Snapshot),
		waiters: make(map[string][]chan workerproto.CommandResult),
		updates: make(chan Update, resolved.UpdateBuffer), wake: make(chan struct{}, 1),
		ready: make(chan struct{}), cancel: cancel,
	}
	client.wait.Add(2)
	go func() {
		defer client.wait.Done()
		client.run(runCtx, connection)
	}()
	go func() {
		defer client.wait.Done()
		client.flushLoop(runCtx)
	}()
	client.signalFlush()
	return client, nil
}

func (client *Client) Updates() <-chan Update { return client.updates }
func (client *Client) Ready() <-chan struct{} { return client.ready }

func (client *Client) WorkerID() string {
	client.cacheMu.RLock()
	defer client.cacheMu.RUnlock()
	return client.workerID
}

func (client *Client) Do(ctx context.Context, command workerproto.Command) (workerproto.CommandResult, error) {
	if client.closing.Load() {
		return workerproto.CommandResult{}, ErrClientClosed
	}
	if err := command.Validate(); err != nil {
		return workerproto.CommandResult{}, err
	}
	waiter := make(chan workerproto.CommandResult, 1)
	client.waiterMu.Lock()
	client.waiters[command.EventID] = append(client.waiters[command.EventID], waiter)
	client.waiterMu.Unlock()
	if _, err := client.outbox.Put(ctx, command); err != nil {
		client.removeWaiter(command.EventID, waiter)
		return workerproto.CommandResult{}, err
	}
	client.signalFlush()
	select {
	case <-ctx.Done():
		client.removeWaiter(command.EventID, waiter)
		return workerproto.CommandResult{}, ctx.Err()
	case result := <-waiter:
		if result.Error != nil {
			return result, decodeProtocolError(result.Error)
		}
		return result, nil
	}
}

func (client *Client) Close() error {
	client.close.Do(func() {
		client.closing.Store(true)
		client.cancel()
		client.writeMu.Lock()
		connection := client.connection
		client.connection = nil
		client.writeMu.Unlock()
		if connection != nil {
			client.closeErr = connection.Close(websocket.StatusNormalClosure, "delegation worker closed")
		}
		client.failWaiters()
		client.wait.Wait()
		if err := client.outbox.Close(); client.closeErr == nil {
			client.closeErr = err
		}
		close(client.updates)
	})
	return client.closeErr
}

func (client *Client) run(ctx context.Context, connection *websocket.Conn) {
	backoff := client.config.ReconnectInitial
	for {
		err := client.readConnection(ctx, connection)
		if client.closing.Load() || ctx.Err() != nil {
			return
		}
		client.writeMu.Lock()
		if client.connection == connection {
			client.connection = nil
			client.inflight = make(map[string]uint64)
		}
		client.writeMu.Unlock()
		client.deliver(ctx, Update{Err: fmt.Errorf("delegation worker connection lost; reconnecting: %w", err)})
		_ = connection.CloseNow()
		for {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			candidate, _, dialErr := websocket.Dial(ctx, client.config.URL, &websocket.DialOptions{
				HTTPHeader: http.Header{"Authorization": []string{"Bearer " + client.config.Token}},
			})
			if dialErr != nil {
				backoff *= 2
				if backoff > client.config.ReconnectMaximum {
					backoff = client.config.ReconnectMaximum
				}
				continue
			}
			client.writeMu.Lock()
			client.connection = candidate
			client.clientSeq = 0
			client.serverSeq.Store(0)
			client.inflight = make(map[string]uint64)
			client.writeMu.Unlock()
			connection = candidate
			backoff = client.config.ReconnectInitial
			client.signalFlush()
			break
		}
	}
}

func (client *Client) readConnection(ctx context.Context, connection *websocket.Conn) error {
	for {
		var envelope workerproto.Envelope
		if err := wsjson.Read(ctx, connection, &envelope); err != nil {
			return err
		}
		if err := envelope.Validate(); err != nil {
			return err
		}
		last := client.serverSeq.Load()
		if envelope.Seq > last+1 {
			return errors.New("delegation server message sequence gap")
		}
		if envelope.Seq <= last {
			if err := client.sendAck(ctx); err != nil {
				return err
			}
			continue
		}
		client.serverSeq.Store(envelope.Seq)
		switch envelope.Type {
		case workerproto.MessageHello:
			var hello workerproto.Hello
			if err := json.Unmarshal(envelope.Payload, &hello); err != nil || strings.TrimSpace(hello.WorkerID) == "" {
				return errors.New("invalid delegation worker hello")
			}
			client.cacheMu.Lock()
			client.workerID = hello.WorkerID
			client.cacheMu.Unlock()
			client.readyOnce.Do(func() { close(client.ready) })
		case workerproto.MessageSnapshot:
			var update workerproto.Snapshot
			if err := json.Unmarshal(envelope.Payload, &update); err != nil {
				return err
			}
			if err := update.Validate(); err != nil {
				return err
			}
			if client.cacheSnapshot(update.Snapshot) {
				client.deliver(ctx, Update{Snapshot: &update})
			}
		case workerproto.MessageTaskRemoved:
			var removed workerproto.TaskRemoved
			if err := json.Unmarshal(envelope.Payload, &removed); err != nil {
				return err
			}
			if err := removed.Validate(); err != nil {
				return err
			}
			client.cacheMu.Lock()
			delete(client.snapshots, removed.TaskID)
			client.cacheMu.Unlock()
			client.deliver(ctx, Update{RemovedTaskID: removed.TaskID})
		case workerproto.MessageCommandResult:
			var result workerproto.CommandResult
			if err := json.Unmarshal(envelope.Payload, &result); err != nil {
				return err
			}
			if err := result.Validate(); err != nil {
				return err
			}
			if err := client.outbox.Delete(ctx, result.EventID); err != nil {
				return err
			}
			client.writeMu.Lock()
			delete(client.inflight, result.EventID)
			client.writeMu.Unlock()
			client.cacheResult(result)
			client.completeWaiters(result)
			client.deliver(ctx, Update{Result: &result})
		case workerproto.MessageAck:
		case workerproto.MessageError:
			var protocolErr workerproto.Error
			_ = json.Unmarshal(envelope.Payload, &protocolErr)
			return fmt.Errorf("delegation worker protocol %s: %s", protocolErr.Code, protocolErr.Message)
		default:
			return errors.New("unexpected delegation server message")
		}
		if err := client.sendAck(ctx); err != nil {
			return err
		}
	}
}

func (client *Client) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-client.wake:
		case <-ticker.C:
		}
		if err := client.flush(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, errConnectionUnavailable) {
			client.deliver(ctx, Update{Err: fmt.Errorf("delegation command delivery failed; retrying: %w", err)})
		}
	}
}

func (client *Client) flush(ctx context.Context) error {
	records, err := client.outbox.List(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := client.sendRecord(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) sendRecord(ctx context.Context, record outboxRecord) error {
	envelope, err := workerproto.NewEnvelope(workerproto.MessageCommand, record.Command)
	if err != nil {
		return err
	}
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if _, sent := client.inflight[record.EventID]; sent {
		return nil
	}
	if client.connection == nil {
		return errConnectionUnavailable
	}
	client.clientSeq++
	sequence := client.clientSeq
	connection := client.connection
	envelope.Version = workerproto.Version
	envelope.Seq = sequence
	envelope.Ack = client.serverSeq.Load()
	client.inflight[record.EventID] = sequence
	writeCtx, cancel := context.WithTimeout(ctx, client.config.WriteTimeout)
	err = wsjson.Write(writeCtx, connection, envelope)
	cancel()
	if err != nil {
		delete(client.inflight, record.EventID)
		_ = connection.CloseNow()
		return err
	}
	return nil
}

func (client *Client) sendAck(ctx context.Context) error {
	envelope, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if client.connection == nil {
		return errConnectionUnavailable
	}
	client.clientSeq++
	envelope.Version = workerproto.Version
	envelope.Seq = client.clientSeq
	envelope.Ack = client.serverSeq.Load()
	writeCtx, cancel := context.WithTimeout(ctx, client.config.WriteTimeout)
	defer cancel()
	return wsjson.Write(writeCtx, client.connection, envelope)
}

func (client *Client) signalFlush() {
	select {
	case client.wake <- struct{}{}:
	default:
	}
}

func (client *Client) deliver(ctx context.Context, update Update) {
	select {
	case client.updates <- update:
	case <-ctx.Done():
	}
}

func (client *Client) completeWaiters(result workerproto.CommandResult) {
	client.waiterMu.Lock()
	waiters := client.waiters[result.EventID]
	delete(client.waiters, result.EventID)
	client.waiterMu.Unlock()
	for _, waiter := range waiters {
		waiter <- result
	}
}

func (client *Client) failWaiters() {
	client.waiterMu.Lock()
	waiters := client.waiters
	client.waiters = make(map[string][]chan workerproto.CommandResult)
	client.waiterMu.Unlock()
	for eventID, channels := range waiters {
		result := workerproto.CommandResult{
			EventID: eventID,
			Error:   &workerproto.Error{Code: "client_closed", Message: ErrClientClosed.Error()},
		}
		for _, waiter := range channels {
			waiter <- result
		}
	}
}

func (client *Client) removeWaiter(eventID string, target chan workerproto.CommandResult) {
	client.waiterMu.Lock()
	defer client.waiterMu.Unlock()
	waiters := client.waiters[eventID]
	for index, waiter := range waiters {
		if waiter != target {
			continue
		}
		waiters = append(waiters[:index], waiters[index+1:]...)
		break
	}
	if len(waiters) == 0 {
		delete(client.waiters, eventID)
	} else {
		client.waiters[eventID] = waiters
	}
}

func (client *Client) cacheResult(result workerproto.CommandResult) {
	var task *delegation.Task
	var event *delegation.Event
	if result.Transition != nil {
		task = &result.Transition.Task
		event = &result.Transition.Event
	} else if result.Delivery != nil {
		task = &result.Delivery.Task
		event = &result.Delivery.Event
	} else if result.Exec != nil {
		task = &result.Exec.Task
		event = &result.Exec.Event
	}
	if task == nil || task.ID == "" {
		return
	}
	client.cacheMu.Lock()
	snapshot := client.snapshots[task.ID]
	if snapshot.Task.ID != "" && snapshot.Task.Revision > task.Revision {
		client.cacheMu.Unlock()
		return
	}
	snapshot.Task = cloneTask(*task)
	if event != nil && (len(snapshot.Events) == 0 || snapshot.Events[len(snapshot.Events)-1].Sequence < event.Sequence) {
		stored := *event
		stored.Data = append([]byte(nil), event.Data...)
		snapshot.Events = append(snapshot.Events, stored)
	}
	if result.Delivery != nil {
		if len(snapshot.Turns) < int(result.Delivery.Turn.Number) {
			snapshot.Turns = append(snapshot.Turns, result.Delivery.Turn)
		}
		found := false
		for index := range snapshot.Artifacts {
			if snapshot.Artifacts[index].TurnNumber == result.Delivery.Artifact.TurnNumber {
				snapshot.Artifacts[index] = cloneArtifact(result.Delivery.Artifact)
				found = true
				break
			}
		}
		if !found {
			snapshot.Artifacts = append(snapshot.Artifacts, cloneArtifact(result.Delivery.Artifact))
		}
	}
	if result.Exec != nil {
		request := cloneExecRequest(result.Exec.Request)
		found := false
		for index := range snapshot.Exec {
			if snapshot.Exec[index].ID == request.ID {
				snapshot.Exec[index] = request
				found = true
				break
			}
		}
		if !found {
			snapshot.Exec = append(snapshot.Exec, request)
		}
	}
	if result.Transition != nil && result.Transition.Event.Kind == delegation.EventRewindConfirmed &&
		result.Transition.Event.TurnNumber != nil {
		target := *result.Transition.Event.TurnNumber
		for index := range snapshot.Turns {
			if snapshot.Turns[index].Number > target && snapshot.Turns[index].SupersededAtRevision == nil {
				revision := result.Transition.Task.Revision
				snapshot.Turns[index].SupersededAtRevision = &revision
			}
		}
		for index := range snapshot.Artifacts {
			if snapshot.Artifacts[index].TurnNumber > target && snapshot.Artifacts[index].SupersededAtRevision == nil {
				revision := result.Transition.Task.Revision
				snapshot.Artifacts[index].SupersededAtRevision = &revision
			}
		}
	}
	client.snapshots[task.ID] = snapshot
	client.cacheMu.Unlock()
}

// cacheSnapshot preserves the authority's monotonic task clock when a
// reconnect recovery frame races a replayed outbox command result.
func (client *Client) cacheSnapshot(snapshot delegation.Snapshot) bool {
	client.cacheMu.Lock()
	defer client.cacheMu.Unlock()
	current, exists := client.snapshots[snapshot.Task.ID]
	if exists && current.Task.Revision > snapshot.Task.Revision {
		return false
	}
	client.snapshots[snapshot.Task.ID] = cloneSnapshot(snapshot)
	return true
}

// The methods below make Client satisfy delegation/worker.Authority while
// retaining the extra lifecycle commands needed directly by the TUI.
func (client *Client) GetTask(_ context.Context, taskID string) (delegation.Task, error) {
	client.cacheMu.RLock()
	defer client.cacheMu.RUnlock()
	snapshot, ok := client.snapshots[taskID]
	if !ok {
		return delegation.Task{}, fmt.Errorf("%w: task %q is not in the worker snapshot", delegation.ErrNotFound, taskID)
	}
	return cloneTask(snapshot.Task), nil
}

func (client *Client) GetArtifact(_ context.Context, taskID string, turn int64) (delegation.Artifact, error) {
	client.cacheMu.RLock()
	defer client.cacheMu.RUnlock()
	snapshot, ok := client.snapshots[taskID]
	if !ok {
		return delegation.Artifact{}, fmt.Errorf("%w: task %q is not in the worker snapshot", delegation.ErrNotFound, taskID)
	}
	for _, artifact := range snapshot.Artifacts {
		if artifact.TurnNumber == turn {
			return cloneArtifact(artifact), nil
		}
	}
	return delegation.Artifact{}, fmt.Errorf("%w: task %q turn %d", delegation.ErrNotFound, taskID, turn)
}

func (client *Client) AcceptTask(ctx context.Context, input delegation.AcceptTaskInput) (delegation.TransitionResult, error) {
	if workerID := client.WorkerID(); workerID != "" && input.WorkerID != "" && input.WorkerID != workerID {
		return delegation.TransitionResult{}, ErrOwnershipConflict
	}
	command, err := makeCommand(workerproto.CommandAccept, input.CommandInput, nil)
	if err != nil {
		return delegation.TransitionResult{}, err
	}
	result, err := client.Do(ctx, command)
	return transitionResult(result, err)
}

func (client *Client) RejectTask(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
	command, err := makeCommand(workerproto.CommandReject, input, nil)
	if err != nil {
		return delegation.TransitionResult{}, err
	}
	result, err := client.Do(ctx, command)
	return transitionResult(result, err)
}

func (client *Client) DeliverTurn(ctx context.Context, input delegation.DeliverTurnInput) (delegation.DeliveryResult, error) {
	delivery := &workerproto.Delivery{
		ArtifactID: input.ArtifactID, ArtifactMediaType: input.ArtifactMediaType,
		ArtifactData:     append([]byte(nil), input.ArtifactData...),
		ArtifactMetadata: append([]byte(nil), input.ArtifactMetadata...),
	}
	command, err := makeCommand(workerproto.CommandDeliver, input.CommandInput, delivery)
	if err != nil {
		return delegation.DeliveryResult{}, err
	}
	result, err := client.Do(ctx, command)
	if err != nil {
		return delegation.DeliveryResult{}, err
	}
	if result.Delivery == nil {
		return delegation.DeliveryResult{}, errors.New("delegation worker response has no delivery result")
	}
	return *result.Delivery, nil
}

// RequestExec proposes a caller-side command. WorkerID is checked locally when
// supplied, but is deliberately absent from the wire payload; humand injects
// the identity authenticated by the bearer token.
func (client *Client) RequestExec(ctx context.Context, input delegation.RequestExecInput) (delegation.ExecResult, error) {
	if workerID := client.WorkerID(); workerID != "" && input.WorkerID != "" && input.WorkerID != workerID {
		return delegation.ExecResult{}, ErrOwnershipConflict
	}
	command, err := makeCommand(workerproto.CommandExec, input.CommandInput, nil)
	if err != nil {
		return delegation.ExecResult{}, err
	}
	command.Exec = &workerproto.ExecRequest{
		RequestID: input.RequestID,
		Command:   input.Command,
		CWD:       input.CWD,
		TimeoutMS: input.TimeoutMS,
		Reason:    input.Reason,
	}
	result, err := client.Do(ctx, command)
	if err != nil {
		return delegation.ExecResult{}, err
	}
	if result.Exec == nil {
		return delegation.ExecResult{}, errors.New("delegation worker response has no exec result")
	}
	return *result.Exec, nil
}

func (client *Client) CompleteTask(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
	command, err := makeCommand(workerproto.CommandComplete, input, nil)
	if err != nil {
		return delegation.TransitionResult{}, err
	}
	result, err := client.Do(ctx, command)
	return transitionResult(result, err)
}

func (client *Client) FailTask(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
	command, err := makeCommand(workerproto.CommandFail, input, nil)
	if err != nil {
		return delegation.TransitionResult{}, err
	}
	result, err := client.Do(ctx, command)
	return transitionResult(result, err)
}

func (client *Client) ConfirmRewind(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
	command, err := makeCommand(workerproto.CommandConfirmRewind, input, nil)
	if err != nil {
		return delegation.TransitionResult{}, err
	}
	result, err := client.Do(ctx, command)
	return transitionResult(result, err)
}

func (client *Client) RejectRewind(ctx context.Context, input delegation.CommandInput) (delegation.TransitionResult, error) {
	command, err := makeCommand(workerproto.CommandRejectRewind, input, nil)
	if err != nil {
		return delegation.TransitionResult{}, err
	}
	result, err := client.Do(ctx, command)
	return transitionResult(result, err)
}

func makeCommand(kind workerproto.CommandKind, input delegation.CommandInput, delivery *workerproto.Delivery) (workerproto.Command, error) {
	eventID, err := newEventID()
	if err != nil {
		return workerproto.Command{}, err
	}
	return workerproto.Command{
		EventID: eventID, Kind: kind, TaskID: input.TaskID,
		ExpectedRevision: input.ExpectedRevision, Data: append([]byte(nil), input.Data...),
		Delivery: delivery,
	}, nil
}

func transitionResult(result workerproto.CommandResult, err error) (delegation.TransitionResult, error) {
	if err != nil {
		return delegation.TransitionResult{}, err
	}
	if result.Transition == nil {
		return delegation.TransitionResult{}, errors.New("delegation worker response has no transition result")
	}
	return *result.Transition, nil
}

func newEventID() (string, error) {
	random := make([]byte, 18)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("read delegation event randomness: %w", err)
	}
	return "dwe_" + base64.RawURLEncoding.EncodeToString(random), nil
}

func decodeProtocolError(protocolErr *workerproto.Error) error {
	var sentinel error
	switch protocolErr.Code {
	case "not_found":
		sentinel = delegation.ErrNotFound
	case "already_exists":
		sentinel = delegation.ErrAlreadyExists
	case "revision_conflict":
		sentinel = delegation.ErrRevisionConflict
	case "idempotency_conflict":
		sentinel = delegation.ErrIdempotencyConflict
	case "invalid_transition":
		sentinel = delegation.ErrInvalidTransition
	case "invalid_rewind":
		sentinel = delegation.ErrInvalidRewind
	case "invalid_input":
		sentinel = delegation.ErrInvalidInput
	case "ownership_conflict":
		sentinel = ErrOwnershipConflict
	case "client_closed":
		sentinel = ErrClientClosed
	default:
		sentinel = errors.New("delegation worker server error")
	}
	return fmt.Errorf("%w: %s", sentinel, protocolErr.Message)
}

func cloneTask(task delegation.Task) delegation.Task {
	task.Metadata = append([]byte(nil), task.Metadata...)
	if task.PendingRewindTo != nil {
		value := *task.PendingRewindTo
		task.PendingRewindTo = &value
	}
	return task
}

func cloneArtifact(artifact delegation.Artifact) delegation.Artifact {
	artifact.Data = append([]byte(nil), artifact.Data...)
	artifact.Metadata = append([]byte(nil), artifact.Metadata...)
	if artifact.SupersededAtRevision != nil {
		value := *artifact.SupersededAtRevision
		artifact.SupersededAtRevision = &value
	}
	return artifact
}

func cloneExecRequest(request delegation.ExecRequest) delegation.ExecRequest {
	request.Stdout = append([]byte(nil), request.Stdout...)
	request.Stderr = append([]byte(nil), request.Stderr...)
	if request.ExitCode != nil {
		value := *request.ExitCode
		request.ExitCode = &value
	}
	if request.ResolutionSequence != nil {
		value := *request.ResolutionSequence
		request.ResolutionSequence = &value
	}
	if request.ResolvedAt != nil {
		value := *request.ResolvedAt
		request.ResolvedAt = &value
	}
	return request
}

func cloneSnapshot(snapshot delegation.Snapshot) delegation.Snapshot {
	snapshot.Task = cloneTask(snapshot.Task)
	snapshot.Turns = append([]delegation.Turn(nil), snapshot.Turns...)
	for index := range snapshot.Turns {
		if snapshot.Turns[index].SupersededAtRevision != nil {
			value := *snapshot.Turns[index].SupersededAtRevision
			snapshot.Turns[index].SupersededAtRevision = &value
		}
	}
	snapshot.Artifacts = append([]delegation.Artifact(nil), snapshot.Artifacts...)
	for index := range snapshot.Artifacts {
		snapshot.Artifacts[index] = cloneArtifact(snapshot.Artifacts[index])
	}
	snapshot.Messages = append([]delegation.Message(nil), snapshot.Messages...)
	for index := range snapshot.Messages {
		snapshot.Messages[index].Data = append([]byte(nil), snapshot.Messages[index].Data...)
	}
	snapshot.Exec = append([]delegation.ExecRequest(nil), snapshot.Exec...)
	for index := range snapshot.Exec {
		snapshot.Exec[index] = cloneExecRequest(snapshot.Exec[index])
	}
	snapshot.Events = append([]delegation.Event(nil), snapshot.Events...)
	for index := range snapshot.Events {
		snapshot.Events[index].Data = append([]byte(nil), snapshot.Events[index].Data...)
		if snapshot.Events[index].TurnNumber != nil {
			value := *snapshot.Events[index].TurnNumber
			snapshot.Events[index].TurnNumber = &value
		}
	}
	return snapshot
}
