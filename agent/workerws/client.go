package workerws

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/vibe-agi/human/agent"
	"github.com/vibe-agi/human/framework"
)

var (
	ErrClientConfiguration  = errors.New("invalid Agent remote worker configuration")
	ErrClientClosed         = errors.New("Agent remote worker client is closed")
	ErrClientAuthentication = errors.New("Agent remote worker authentication was rejected")
	ErrClientProtocol       = errors.New("Agent remote worker protocol failed")
)

const (
	defaultClientReadLimit    int64 = 72 << 20
	defaultNotificationBuffer       = 64
	defaultJournalPageSize          = 256
)

// HeaderProvider supplies per-dial authentication headers. It may refresh a
// short-lived credential on every reconnect. Returned maps are cloned before
// use and SessionHeader is always overwritten by Client.
type HeaderProvider interface {
	WorkerHeaders(context.Context) (http.Header, error)
}

type HeaderProviderFunc func(context.Context) (http.Header, error)

func (function HeaderProviderFunc) WorkerHeaders(ctx context.Context) (http.Header, error) {
	return function(ctx)
}

// ClientConfig configures the official remote Agent worker. Journal is the
// only durability boundary and must implement JournalContractMajor exactly.
// URL must use ws or wss and must not contain userinfo or a fragment.
type ClientConfig struct {
	URL       string
	Gateway   GatewayID
	Authority agent.AuthorityID
	Worker    agent.WorkerID
	Journal   framework.Resource[Journal]

	HTTPHeader     http.Header
	HeaderProvider HeaderProvider
	HTTPClient     *http.Client

	ConnectTimeout      time.Duration
	WriteTimeout        time.Duration
	ReadLimit           int64
	ReconnectMinDelay   time.Duration
	ReconnectMaxDelay   time.Duration
	ReconnectResetAfter time.Duration
	ReleaseTimeout      time.Duration
	NotificationBuffer  int
}

type resolvedClientConfig struct {
	ClientConfig
	journal   Journal
	parsedURL string
}

func (config ClientConfig) resolve(ctx context.Context) (resolvedClientConfig, error) {
	if ctx == nil {
		return resolvedClientConfig{}, fmt.Errorf("%w: context is required", ErrClientConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return resolvedClientConfig{}, err
	}
	parsed, err := url.Parse(config.URL)
	if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return resolvedClientConfig{}, fmt.Errorf("%w: URL must be an absolute ws/wss URL without userinfo or fragment", ErrClientConfiguration)
	}
	if strings.TrimSpace(config.URL) != config.URL {
		return resolvedClientConfig{}, fmt.Errorf("%w: URL contains surrounding whitespace", ErrClientConfiguration)
	}
	if err := config.Gateway.Validate(); err != nil {
		return resolvedClientConfig{}, fmt.Errorf("%w: %v", ErrClientConfiguration, err)
	}
	probe := agent.AuthenticatedWorker{Authority: config.Authority, Worker: config.Worker, Session: "configuration-probe"}
	if err := probe.Validate(); err != nil {
		return resolvedClientConfig{}, fmt.Errorf("%w: %v", ErrClientConfiguration, err)
	}
	journal, err := config.Journal.Value()
	if err != nil {
		return resolvedClientConfig{}, fmt.Errorf("%w: journal resource: %v", ErrClientConfiguration, err)
	}
	if nilJournal(journal) {
		return resolvedClientConfig{}, fmt.Errorf("%w: journal is required", ErrClientConfiguration)
	}
	if err := journal.Description().Validate(); err != nil {
		return resolvedClientConfig{}, fmt.Errorf("%w: journal: %v", ErrClientConfiguration, err)
	}
	if nilHeaderProvider(config.HeaderProvider) {
		return resolvedClientConfig{}, fmt.Errorf("%w: header provider is typed nil", ErrClientConfiguration)
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.ReadLimit == 0 {
		config.ReadLimit = defaultClientReadLimit
	}
	if config.ReconnectMinDelay == 0 {
		config.ReconnectMinDelay = 100 * time.Millisecond
	}
	if config.ReconnectMaxDelay == 0 {
		config.ReconnectMaxDelay = 10 * time.Second
	}
	if config.ReconnectResetAfter == 0 {
		config.ReconnectResetAfter = 30 * time.Second
	}
	if config.ReleaseTimeout == 0 {
		config.ReleaseTimeout = 10 * time.Second
	}
	if config.NotificationBuffer == 0 {
		config.NotificationBuffer = defaultNotificationBuffer
	}
	if config.ConnectTimeout <= 0 || config.WriteTimeout <= 0 || config.ReleaseTimeout <= 0 ||
		config.ReconnectMinDelay <= 0 || config.ReconnectMaxDelay < config.ReconnectMinDelay ||
		config.ReconnectResetAfter <= 0 {
		return resolvedClientConfig{}, fmt.Errorf("%w: durations and reconnect bounds must be positive", ErrClientConfiguration)
	}
	if config.ReadLimit < 1024 || config.ReadLimit > defaultClientReadLimit {
		return resolvedClientConfig{}, fmt.Errorf("%w: read limit must be 1024..%d", ErrClientConfiguration, defaultClientReadLimit)
	}
	if config.NotificationBuffer < 1 || config.NotificationBuffer > 4096 {
		return resolvedClientConfig{}, fmt.Errorf("%w: notification buffer must be 1..4096", ErrClientConfiguration)
	}
	if err := journal.Bind(ctx, JournalBinding{
		Gateway: config.Gateway, Authority: config.Authority, Worker: config.Worker,
	}); err != nil {
		return resolvedClientConfig{}, fmt.Errorf("%w: bind journal: %w", ErrClientConfiguration, err)
	}
	config.HTTPHeader = config.HTTPHeader.Clone()
	return resolvedClientConfig{ClientConfig: config, journal: journal, parsedURL: parsed.String()}, nil
}

// Client is a durable remote Agent worker runtime. Assignments and Rejections
// are replayed from Journal after process restart until explicitly confirmed.
type Client struct {
	config resolvedClientConfig
	ctx    context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}

	assignments    chan agent.WorkerAssignmentDelivery
	rejections     chan RejectedEvent
	eventWake      chan struct{}
	assignmentWake chan struct{}
	rejectionWake  chan struct{}

	presentMu          sync.Mutex
	presentAssignments map[agent.WorkerDeliveryID]JournalDigest
	presentRejections  map[agent.WorkerDeliveryID]JournalDigest

	errMu        sync.RWMutex
	err          error
	shutdownOnce sync.Once

	operationMu      sync.Mutex
	operationCond    *sync.Cond
	accepting        bool
	activeOperations uint64
}

var _ framework.Runtime = (*Client)(nil)

// NewClient validates composition and starts reconnect and replay loops. ctx
// bounds construction only; Shutdown owns the successful runtime lifetime.
func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	resolved, err := config.resolve(ctx)
	if err != nil {
		releaseFailedClientResource(config.Journal, config.ReleaseTimeout)
		return nil, err
	}
	lifecycle, cancel := context.WithCancelCause(context.Background())
	client := &Client{
		config: resolved, ctx: lifecycle, cancel: cancel, done: make(chan struct{}),
		assignments: make(chan agent.WorkerAssignmentDelivery, resolved.NotificationBuffer),
		rejections:  make(chan RejectedEvent, resolved.NotificationBuffer),
		eventWake:   make(chan struct{}, 1), assignmentWake: make(chan struct{}, 1), rejectionWake: make(chan struct{}, 1),
		presentAssignments: make(map[agent.WorkerDeliveryID]JournalDigest),
		presentRejections:  make(map[agent.WorkerDeliveryID]JournalDigest),
		accepting:          true,
	}
	client.operationCond = sync.NewCond(&client.operationMu)
	go client.run()
	return client, nil
}

func (client *Client) Assignments() <-chan agent.WorkerAssignmentDelivery { return client.assignments }
func (client *Client) Rejections() <-chan RejectedEvent                   { return client.rejections }
func (client *Client) Done() <-chan struct{}                              { return client.done }

func (client *Client) Err() error {
	select {
	case <-client.done:
		client.errMu.RLock()
		defer client.errMu.RUnlock()
		return client.err
	default:
		return nil
	}
}

// SendEvent commits an exact event to the durable outbox before making it
// eligible for transmission. Exact settled replay is a no-op; ID reuse with a
// different payload returns ErrJournalConflict.
func (client *Client) SendEvent(ctx context.Context, delivery agent.WorkerEventDelivery) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrClientConfiguration)
	}
	if err := client.beginOperation(); err != nil {
		return err
	}
	defer client.endOperation()
	if err := delivery.ValidateFor(agent.AuthenticatedWorker{
		Authority: client.config.Authority, Worker: client.config.Worker, Session: "send-validation",
	}); err != nil {
		return err
	}
	digest, err := digestJournalValue(delivery)
	if err != nil {
		return err
	}
	state, err := client.config.journal.PutEvent(ctx, JournalEvent{
		Digest: digest, Delivery: agent.CloneWorkerEventDelivery(delivery),
	})
	if err != nil {
		return &JournalError{Operation: "put event", Delivery: delivery.ID, Cause: err}
	}
	if state != JournalEntryPending && state != JournalEntrySettled {
		return &JournalError{Operation: "put event", Delivery: delivery.ID, Cause: ErrJournalCorrupt}
	}
	if state == JournalEntryPending {
		signal(client.eventWake)
	}
	return nil
}

// ConfirmAssignment removes an assignment from the application inbox and
// retains a compact digest tombstone so a lost wire ACK cannot re-present it.
func (client *Client) ConfirmAssignment(ctx context.Context, delivery agent.WorkerDeliveryID) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrClientConfiguration)
	}
	if err := client.beginOperation(); err != nil {
		return err
	}
	defer client.endOperation()
	if err := client.config.journal.ConfirmAssignment(ctx, delivery); err != nil {
		return &JournalError{Operation: "confirm assignment", Delivery: delivery, Cause: err}
	}
	client.presentMu.Lock()
	delete(client.presentAssignments, delivery)
	client.presentMu.Unlock()
	return nil
}

// ConfirmRejection removes one NACK from the application inbox. Its compact
// event settlement tombstone remains for exact replay/conflict detection.
func (client *Client) ConfirmRejection(ctx context.Context, delivery agent.WorkerDeliveryID) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrClientConfiguration)
	}
	if err := client.beginOperation(); err != nil {
		return err
	}
	defer client.endOperation()
	if err := client.config.journal.ConfirmRejection(ctx, delivery); err != nil {
		return &JournalError{Operation: "confirm rejection", Delivery: delivery, Cause: err}
	}
	client.presentMu.Lock()
	delete(client.presentRejections, delivery)
	client.presentMu.Unlock()
	return nil
}

func (client *Client) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is required", ErrClientConfiguration)
	}
	client.shutdownOnce.Do(func() { client.stopAdmission(ErrClientClosed) })
	select {
	case <-client.done:
		return client.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (client *Client) beginOperation() error {
	client.operationMu.Lock()
	defer client.operationMu.Unlock()
	if !client.accepting {
		return ErrClientClosed
	}
	client.activeOperations++
	return nil
}

func (client *Client) endOperation() {
	client.operationMu.Lock()
	client.activeOperations--
	if client.activeOperations == 0 {
		client.operationCond.Broadcast()
	}
	client.operationMu.Unlock()
}

func (client *Client) stopAdmission(cause error) {
	client.operationMu.Lock()
	client.accepting = false
	client.operationMu.Unlock()
	client.cancel(cause)

}

func (client *Client) waitOperations() {
	client.operationMu.Lock()
	for client.activeOperations != 0 {
		client.operationCond.Wait()
	}
	client.operationMu.Unlock()
}

func (client *Client) run() {
	var pumps sync.WaitGroup
	pumps.Add(2)
	go func() { defer pumps.Done(); client.assignmentPump() }()
	go func() { defer pumps.Done(); client.rejectionPump() }()

	connectionErr := client.connectionLoop()
	if connectionErr != nil {
		client.stopAdmission(connectionErr)
	} else if client.ctx.Err() != nil {
		client.stopAdmission(context.Cause(client.ctx))
	}
	pumps.Wait()
	client.waitOperations()
	close(client.assignments)
	close(client.rejections)

	terminal := context.Cause(client.ctx)
	if errors.Is(terminal, ErrClientClosed) {
		terminal = nil
	}
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), client.config.ReleaseTimeout)
	releaseErr := client.config.Journal.Release(releaseCtx)
	releaseCancel()
	if releaseErr != nil {
		terminal = errors.Join(terminal, fmt.Errorf("release Agent worker journal: %w", releaseErr))
	}
	client.errMu.Lock()
	client.err = terminal
	client.errMu.Unlock()
	close(client.done)
}

func releaseFailedClientResource(resource framework.Resource[Journal], timeout time.Duration) {
	if !resource.Owned() {
		return
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = resource.Release(ctx)
}

func nilJournal(journal Journal) bool {
	if journal == nil {
		return true
	}
	value := reflect.ValueOf(journal)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func nilHeaderProvider(provider HeaderProvider) bool {
	if provider == nil {
		return false
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func digestJournalValue(value any) (JournalDigest, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode Agent worker journal value: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return JournalDigest(hex.EncodeToString(digest[:])), nil
}

func signal(channel chan<- struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func newWorkerSessionID() (agent.WorkerSessionID, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("create Agent worker session identity: %w", err)
	}
	return agent.WorkerSessionID("session-" + hex.EncodeToString(random[:])), nil
}

func permanentClientError(err error) error { return &clientPermanentError{cause: err} }

type clientPermanentError struct{ cause error }

func (failure *clientPermanentError) Error() string { return failure.cause.Error() }
func (failure *clientPermanentError) Unwrap() error { return failure.cause }

func isPermanentClientError(err error) bool {
	var permanent *clientPermanentError
	return errors.As(err, &permanent)
}

func journalFailurePermanent(err error) bool {
	return errors.Is(err, ErrJournalClosed) || errors.Is(err, ErrJournalCorrupt) || errors.Is(err, ErrJournalConflict)
}
