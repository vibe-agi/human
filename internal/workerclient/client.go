// Package workerclient implements the human TUI side of the private worker WS.
package workerclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/vibe-agi/human/internal/completion"
	"github.com/vibe-agi/human/internal/completion/canonical"
	"github.com/vibe-agi/human/internal/workerproto"
)

var errWorkerConnectionUnavailable = errors.New("worker connection is not available")

type Message struct {
	Assignment *completion.Assignment
	Err        error
}

type Client struct {
	connection *websocket.Conn
	cancel     context.CancelFunc
	messages   chan Message
	writeMu    sync.Mutex
	clientSeq  uint64
	serverSeq  atomic.Uint64
	inflight   map[string]uint64
	workerMu   sync.RWMutex
	workerID   string
	closing    atomic.Bool
	closeOnce  sync.Once
	wait       sync.WaitGroup
	wake       chan struct{}
	url        string
	token      string
	outbox     *durableOutbox
	closeErr   error
}

// Dial uses the private default outbox. Call DialWithOutbox when embedding the
// client or when the path comes from explicit configuration.
func Dial(ctx context.Context, url, token string) (*Client, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve worker outbox home: %w", err)
	}
	return DialWithOutbox(ctx, url, token, filepath.Join(home, ".human", "worker-outbox.db"))
}

// DialWithOutbox connects a worker and binds it to a durable event outbox.
// The bearer token is never stored; only a SHA-256 namespace derived from the
// endpoint and token is written so another credential cannot replay events.
func DialWithOutbox(ctx context.Context, url, token, outboxPath string) (*Client, error) {
	outbox, err := openDurableOutbox(ctx, outboxPath, url, token)
	if err != nil {
		return nil, err
	}
	connection, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}},
	})
	if err != nil {
		_ = outbox.Close()
		return nil, err
	}
	runContext, cancel := context.WithCancel(context.Background())
	client := &Client{
		connection: connection,
		cancel:     cancel,
		messages:   make(chan Message, 64),
		inflight:   make(map[string]uint64),
		wake:       make(chan struct{}, 1),
		url:        url,
		token:      token,
		outbox:     outbox,
	}
	client.wait.Add(2)
	go func() {
		defer client.wait.Done()
		client.run(runContext, connection)
	}()
	go func() {
		defer client.wait.Done()
		client.flushLoop(runContext)
	}()
	client.signalFlush()
	return client, nil
}

func (client *Client) Messages() <-chan Message {
	return client.messages
}

func (client *Client) WorkerID() string {
	client.workerMu.RLock()
	defer client.workerMu.RUnlock()
	return client.workerID
}

// SendEvent returns success once the exact event is committed to the local
// outbox. Network delivery and ACK may happen later. Consequently a transient
// socket failure cannot make the TUI generate a second event ID for an event
// that is already durable locally.
func (client *Client) SendEvent(ctx context.Context, assignment completion.Assignment, event completion.Event) error {
	if event.ID == "" {
		var err error
		event.ID, err = canonical.NewOpaqueID("event_")
		if err != nil {
			return err
		}
	}
	if event.Type == completion.EventAccepted && event.WorkerID == "" {
		event.WorkerID = client.WorkerID()
		if event.WorkerID == "" {
			return errors.New("worker identity is not established")
		}
	}
	if _, err := client.outbox.Put(ctx, assignment, event); err != nil {
		return err
	}
	client.signalFlush()
	return nil
}

func (client *Client) Close() error {
	client.closeOnce.Do(func() {
		client.closing.Store(true)
		client.cancel()
		client.writeMu.Lock()
		connection := client.connection
		client.connection = nil
		client.writeMu.Unlock()
		if connection != nil {
			client.closeErr = connection.Close(websocket.StatusNormalClosure, "human TUI closed")
		}
		client.wait.Wait()
		if err := client.outbox.Close(); client.closeErr == nil {
			client.closeErr = err
		}
	})
	return client.closeErr
}

func (client *Client) run(ctx context.Context, connection *websocket.Conn) {
	defer close(client.messages)
	backoff := 100 * time.Millisecond
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
		client.deliver(Message{Err: fmt.Errorf("worker connection lost; reconnecting: %w", err)})
		_ = connection.CloseNow()
		for {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			candidate, _, dialErr := websocket.Dial(ctx, client.url, &websocket.DialOptions{
				HTTPHeader: http.Header{"Authorization": []string{"Bearer " + client.token}},
			})
			if dialErr != nil {
				if backoff < 5*time.Second {
					backoff *= 2
					if backoff > 5*time.Second {
						backoff = 5 * time.Second
					}
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
			backoff = 100 * time.Millisecond
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
			return errors.New("server worker-protocol sequence gap")
		}
		if err := client.acknowledge(ctx, envelope.Ack); err != nil {
			return err
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
			if err := json.Unmarshal(envelope.Payload, &hello); err != nil {
				return err
			}
			client.workerMu.Lock()
			client.workerID = hello.WorkerID
			client.workerMu.Unlock()
		case workerproto.MessageAssignment:
			var assignment completion.Assignment
			if err := json.Unmarshal(envelope.Payload, &assignment); err != nil {
				return err
			}
			client.deliver(Message{Assignment: &assignment})
		case workerproto.MessageAck:
		case workerproto.MessageError:
			var protocolError workerproto.Error
			_ = json.Unmarshal(envelope.Payload, &protocolError)
			return errors.New(protocolError.Code + ": " + protocolError.Message)
		default:
			return errors.New("unexpected server worker message")
		}
		if err := client.sendAck(ctx); err != nil {
			return err
		}
	}
}

func (client *Client) acknowledge(ctx context.Context, sequence uint64) error {
	if sequence == 0 {
		return nil
	}
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	for eventID, sentAt := range client.inflight {
		if sentAt > sequence {
			continue
		}
		if err := client.outbox.Delete(ctx, eventID); err != nil {
			return err
		}
		delete(client.inflight, eventID)
	}
	return nil
}

func (client *Client) sendAck(ctx context.Context) error {
	envelope, _ := workerproto.NewEnvelope(workerproto.MessageAck, nil)
	return client.write(ctx, envelope)
}

func (client *Client) write(ctx context.Context, envelope workerproto.Envelope) error {
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if client.connection == nil {
		return errWorkerConnectionUnavailable
	}
	client.clientSeq++
	envelope.Version = workerproto.Version
	envelope.Seq = client.clientSeq
	envelope.Ack = client.serverSeq.Load()
	writeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return wsjson.Write(writeContext, client.connection, envelope)
}

func (client *Client) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	reportedFailure := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-client.wake:
		case <-ticker.C:
		}
		err := client.flush(ctx)
		if err == nil {
			reportedFailure = false
			continue
		}
		if ctx.Err() == nil && !errors.Is(err, errWorkerConnectionUnavailable) && !reportedFailure {
			reportedFailure = true
			client.deliver(Message{Err: fmt.Errorf("worker outbox delivery failed; retrying: %w", err)})
		}
	}
}

func (client *Client) flush(ctx context.Context) error {
	records, err := client.outbox.List(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := client.sendOutboxRecord(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) sendOutboxRecord(ctx context.Context, record outboxRecord) error {
	envelope, err := workerproto.NewEnvelope(workerproto.MessageEvent, record.Message)
	if err != nil {
		return err
	}
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if _, sent := client.inflight[record.EventID]; sent {
		return nil
	}
	if client.connection == nil {
		return errWorkerConnectionUnavailable
	}
	connection := client.connection
	client.clientSeq++
	envelope.Version = workerproto.Version
	envelope.Seq = client.clientSeq
	envelope.Ack = client.serverSeq.Load()
	client.inflight[record.EventID] = envelope.Seq
	writeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = wsjson.Write(writeContext, connection, envelope)
	cancel()
	if err != nil {
		delete(client.inflight, record.EventID)
		_ = connection.CloseNow()
		return err
	}
	return nil
}

func (client *Client) signalFlush() {
	select {
	case client.wake <- struct{}{}:
	default:
	}
}

func (client *Client) deliver(message Message) {
	select {
	case client.messages <- message:
	default:
		client.cancel()
	}
}
