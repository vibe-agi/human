package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"reflect"
	"sync"
	"time"

	human "github.com/vibe-agi/human"
	"github.com/vibe-agi/human/examples/custom-framework/customprotect"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	llmsqlite "github.com/vibe-agi/human/llm/sqlite"
	protectaead "github.com/vibe-agi/human/protect/aead"
)

var (
	errAuthentication   = errors.New("custom transport: authentication failed")
	errTransportClosed  = errors.New("custom transport: runtime is closed")
	errAlreadyStarted   = errors.New("custom transport: already started")
	errEndpointProtocol = errors.New("custom transport: endpoint violated response protocol")
)

// auditedStore is an application-owned Store decorator. It deliberately
// advertises the application's provider identity while forwarding the complete
// storage contract to an official SQLite adapter.
type auditedStore struct {
	next        llm.Store
	description llm.StoreDescription
	log         *auditLog
}

type auditLog struct {
	out io.Writer
	mu  sync.Mutex
}

func (audit *auditLog) record(message string) {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	_, _ = fmt.Fprintf(audit.out, "audit %s\n", message)
}

func (store *auditedStore) Description() llm.StoreDescription {
	description := store.description
	description.Contract.Features = maps.Clone(description.Contract.Features)
	return description
}

func (store *auditedStore) Bind(ctx context.Context, binding llm.StoreBinding) error {
	store.audit("bind")
	return store.next.Bind(ctx, binding)
}

func (store *auditedStore) View(ctx context.Context, view func(llm.StoreView) error) error {
	store.audit("view")
	return store.next.View(ctx, view)
}

func (store *auditedStore) Update(ctx context.Context, update func(llm.StoreTx) error) error {
	store.audit("update")
	return store.next.Update(ctx, update)
}

func (store *auditedStore) audit(operation string) {
	store.log.record("store." + operation)
}

// openAuditedSQLite transfers the SQLite resource into a new owned decorator
// resource. HumanLLM releases the decorator, whose callback releases SQLite;
// the host must not also close either object.
func openAuditedSQLite(
	ctx context.Context,
	path string,
	audit *auditLog,
) (framework.Resource[llm.Store], error) {
	baseResource, err := llmsqlite.Open(ctx, llmsqlite.Config{Path: path})
	if err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	base, err := baseResource.Value()
	if err != nil {
		_ = releaseResource(baseResource)
		return framework.Resource[llm.Store]{}, err
	}
	baseDescription := base.Description()
	if err := baseDescription.Validate(); err != nil {
		return framework.Resource[llm.Store]{}, errors.Join(
			err,
			releaseResource(baseResource),
		)
	}
	contract, err := framework.Negotiate(baseDescription.Contract, llm.StoreRequirements())
	if err != nil {
		return framework.Resource[llm.Store]{}, errors.Join(
			err,
			releaseResource(baseResource),
		)
	}
	resource, err := framework.Own[llm.Store](
		&auditedStore{
			next: base,
			description: llm.StoreDescription{
				Contract: contract, Provider: "example.audited-sqlite", Version: "1",
			},
			log: audit,
		},
		baseResource.Release,
	)
	if err != nil {
		return framework.Resource[llm.Store]{}, errors.Join(
			err,
			releaseResource(baseResource),
		)
	}
	return resource, nil
}

// authenticator belongs to this adapter, not to HumanLLM core. A real adapter
// can replace it with an application's account, session, or mTLS authority.
type authenticator interface {
	Authenticate(context.Context, string) (llm.CallerID, error)
}

type tokenAuthenticator struct {
	tokenDigest [sha256.Size]byte
	caller      llm.CallerID
}

func newTokenAuthenticator(token string, caller llm.CallerID) *tokenAuthenticator {
	return &tokenAuthenticator{tokenDigest: sha256.Sum256([]byte(token)), caller: caller}
}

func (auth *tokenAuthenticator) Authenticate(ctx context.Context, token string) (llm.CallerID, error) {
	if ctx == nil {
		return "", errAuthentication
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	candidate := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(candidate[:], auth.tokenDigest[:]) != 1 {
		return "", errAuthentication
	}
	return auth.caller, nil
}

// call is the application's non-HTTP wire request. CallerID is intentionally
// absent: only authenticator may construct that authority.
type call struct {
	Token          string
	IdempotencyKey llm.IdempotencyKey
	CodecID        llm.CodecID
	Body           []byte
	Task           llm.TaskContext
}

type callResult struct {
	Identity      llm.CompletionIdentity
	RequestDigest llm.StoreDigest
	StatusCode    int
	ContentType   string
	Body          []byte
	Frames        [][]byte
}

// inProcessTransport is a complete CallerTransport whose "wire" is a Go
// method call. It owns its runtime but only borrows auth and CallerEndpoint.
type inProcessTransport struct {
	auth authenticator

	mu      sync.RWMutex
	runtime *inProcessRuntime
	started bool
}

var _ llm.CallerTransport = (*inProcessTransport)(nil)

func newInProcessTransport(auth authenticator) *inProcessTransport {
	return &inProcessTransport{auth: auth}
}

func (*inProcessTransport) Description() llm.CallerTransportDescription {
	return llm.CallerTransportDescription{
		Contract: framework.Contract{
			ID: llm.CallerTransportContractID, Major: llm.CallerTransportContractMajor,
		},
		Provider: "example.authenticated-in-process",
		Version:  "1",
	}
}

func (transport *inProcessTransport) Start(
	ctx context.Context,
	endpoint llm.CallerEndpoint,
) (llm.CallerTransportRuntime, error) {
	if ctx == nil {
		return nil, errors.New("custom transport: start context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if transport == nil || nilInterface(transport.auth) || nilInterface(endpoint) {
		return nil, errors.New("custom transport: authenticator and endpoint are required")
	}
	if _, err := llm.ValidateCallerTransport(transport); err != nil {
		return nil, err
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.started {
		return nil, errAlreadyStarted
	}
	transport.started = true
	lifecycle, cancel := context.WithCancel(context.Background())
	runtime := &inProcessRuntime{
		auth: transport.auth, endpoint: endpoint,
		lifecycle: lifecycle, cancel: cancel, done: make(chan struct{}),
	}
	transport.runtime = runtime
	return runtime, nil
}

func (transport *inProcessTransport) Call(ctx context.Context, request call) (callResult, error) {
	transport.mu.RLock()
	runtime := transport.runtime
	transport.mu.RUnlock()
	if runtime == nil {
		return callResult{}, errTransportClosed
	}
	return runtime.call(ctx, request)
}

type inProcessRuntime struct {
	auth     authenticator
	endpoint llm.CallerEndpoint

	lifecycle context.Context
	cancel    context.CancelFunc
	done      chan struct{}

	mu           sync.Mutex
	closing      bool
	runtimeError error
	active       sync.WaitGroup
	shutdownOnce sync.Once
}

var _ llm.CallerTransportRuntime = (*inProcessRuntime)(nil)

func (runtime *inProcessRuntime) Done() <-chan struct{} { return runtime.done }

func (runtime *inProcessRuntime) Err() error {
	select {
	case <-runtime.done:
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.runtimeError
	default:
		return nil
	}
}

func (runtime *inProcessRuntime) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("custom transport: shutdown context is required")
	}
	runtime.shutdownOnce.Do(func() {
		runtime.mu.Lock()
		runtime.closing = true
		runtime.mu.Unlock()
		runtime.cancel()
		go func() {
			runtime.active.Wait()
			close(runtime.done)
		}()
	})
	select {
	case <-runtime.done:
		return runtime.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (runtime *inProcessRuntime) begin() (func(), error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closing {
		return nil, errTransportClosed
	}
	runtime.active.Add(1)
	return runtime.active.Done, nil
}

func (runtime *inProcessRuntime) call(ctx context.Context, request call) (callResult, error) {
	if ctx == nil {
		return callResult{}, errors.New("custom transport: call context is required")
	}
	end, err := runtime.begin()
	if err != nil {
		return callResult{}, err
	}
	defer end()

	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(runtime.lifecycle, cancel)
	defer func() {
		stop()
		cancel()
	}()

	caller, err := runtime.auth.Authenticate(operation, request.Token)
	if err != nil {
		return callResult{}, err
	}
	admission, err := runtime.endpoint.Admit(operation, llm.AdmissionRequest{
		CallerID: caller, IdempotencyKey: request.IdempotencyKey,
		CodecID: request.CodecID, Body: request.Body, Task: request.Task,
	})
	if err != nil {
		return callResult{}, err
	}
	if err := validateAdmission(admission, caller, request.IdempotencyKey); err != nil {
		return callResult{}, err
	}

	result := callResult{Identity: admission.Identity, RequestDigest: admission.RequestDigest}
	page := admission.Response
	mode := page.Mode
	after := uint64(0)
	waited := false
	for {
		if err := validatePage(page, admission.Identity, admission.RequestDigest, mode, after, waited); err != nil {
			return callResult{}, err
		}
		mergePage(&result, page)
		if page.Complete {
			return result, nil
		}
		after = page.Cursor
		page, err = runtime.endpoint.WaitResponse(operation, llm.ResponseQuery{
			CallerID: caller, IdempotencyKey: request.IdempotencyKey,
			RequestDigest: admission.RequestDigest, After: after,
		})
		if err != nil {
			return callResult{}, err
		}
		waited = true
	}
}

func validateAdmission(
	admission llm.AdmissionResult,
	caller llm.CallerID,
	key llm.IdempotencyKey,
) error {
	if err := admission.Identity.Validate(); err != nil {
		return fmt.Errorf("%w: invalid admission identity: %v", errEndpointProtocol, err)
	}
	if admission.Identity.CallerID != caller || admission.Identity.IdempotencyKey != key {
		return fmt.Errorf("%w: admission authority changed", errEndpointProtocol)
	}
	if admission.RequestDigest == "" {
		return fmt.Errorf("%w: admission digest is empty", errEndpointProtocol)
	}
	if admission.Response.Identity != admission.Identity ||
		admission.Response.RequestDigest != admission.RequestDigest {
		return fmt.Errorf("%w: admission response identity changed", errEndpointProtocol)
	}
	return nil
}

func validatePage(
	page llm.ResponsePage,
	identity llm.CompletionIdentity,
	digest llm.StoreDigest,
	mode llm.ResponseMode,
	after uint64,
	waited bool,
) error {
	if page.Identity != identity || page.RequestDigest != digest {
		return fmt.Errorf("%w: response page identity changed", errEndpointProtocol)
	}
	if page.Mode != mode || (page.Mode != llm.ResponseAggregate && page.Mode != llm.ResponseStream) {
		return fmt.Errorf("%w: response mode changed or is invalid", errEndpointProtocol)
	}
	if page.Cursor < after {
		return fmt.Errorf("%w: response cursor moved backwards", errEndpointProtocol)
	}
	if waited && !page.Complete && page.Cursor == after {
		return fmt.Errorf("%w: incomplete wait made no progress", errEndpointProtocol)
	}
	sequence := after
	for _, event := range page.Events {
		if event.Sequence <= sequence || event.Sequence > page.Cursor {
			return fmt.Errorf("%w: response event sequence is unordered", errEndpointProtocol)
		}
		sequence = event.Sequence
	}
	if page.Complete && !page.DecisionCommitted {
		return fmt.Errorf("%w: response completed without a decision", errEndpointProtocol)
	}
	return nil
}

func mergePage(result *callResult, page llm.ResponsePage) {
	if page.DecisionCommitted {
		result.StatusCode = page.Decision.StatusCode
		result.ContentType = page.Decision.ContentType
		result.Body = append(result.Body[:0], page.Decision.Body...)
	}
	for _, event := range page.Events {
		result.Frames = append(result.Frames, append([]byte(nil), event.Data...))
	}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func run(ctx context.Context, out io.Writer) (resultErr error) {
	if ctx == nil || out == nil {
		return errors.New("example requires a context and output")
	}

	audit := &auditLog{out: out}
	storeResource, err := openAuditedSQLite(ctx, ":memory:", audit)
	if err != nil {
		return err
	}
	keyMaterial := make([]byte, protectaead.KeySize)
	if _, err := rand.Read(keyMaterial); err != nil {
		clear(keyMaterial)
		return errors.Join(err, releaseResource(storeResource))
	}
	protectorResource, protectorErr := customprotect.OpenLocal(ctx, protectaead.Config{
		Active: protectaead.KeyRef{ID: "example-key", Version: "1"},
		Keys: []protectaead.Key{{
			ID: "example-key", Version: "1", Material: keyMaterial,
		}},
	}, func(event customprotect.AuditEvent) {
		audit.record("protector." + event.Operation)
	})
	clear(keyMaterial)
	if protectorErr != nil {
		return errors.Join(protectorErr, releaseResource(storeResource))
	}
	config := human.DefaultLLMConfig()
	config.DeploymentID = "custom-framework-example"
	config.Store = storeResource
	config.Protector = protectorResource
	runtime, err := human.NewLLM(ctx, config)
	if err != nil {
		// NewLLM owns cleanup after the constructor boundary, including failure.
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, shutdown(runtime)) }()

	worker, err := runtime.OpenWorker(ctx, llm.AuthenticatedWorker{
		WorkerID: "embedded-human", SessionID: "local-session",
	})
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, shutdown(worker)) }()

	transport := newInProcessTransport(newTokenAuthenticator("demo-secret", "example-caller"))
	callerRuntime, err := transport.Start(ctx, runtime)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, shutdown(callerRuntime)) }()

	type callOutcome struct {
		result callResult
		err    error
	}
	completed := make(chan callOutcome, 1)
	go func() {
		response, callErr := transport.Call(ctx, call{
			Token: "demo-secret", IdempotencyKey: "example-request-1",
			CodecID: "openai.chat",
			Body: []byte(`{
				"model":"human-expert",
				"stream":false,
				"messages":[{"role":"user","content":"Please review this workspace."}]
			}`),
			Task: llm.TaskContext{CapabilityTier: llm.TierChat},
		})
		completed <- callOutcome{result: response, err: callErr}
	}()

	var assignment llm.WorkerAssignmentDelivery
	select {
	case assignment = <-worker.Assignments():
	case <-ctx.Done():
		return ctx.Err()
	}
	// This worker is embedded in the same failure domain as HumanLLM and uses the
	// core's durable assignment as its journal. A remote worker would persist the
	// exact delivery in its own journal before crossing this ACK boundary.
	if err := worker.AckAssignment(ctx, assignment.ID); err != nil {
		return err
	}
	receipt, err := worker.CommitEvent(ctx, llm.WorkerEventDelivery{
		ID:       "embedded-final-delivery",
		Identity: assignment.Assignment.Identity,
		LeaseID:  assignment.Assignment.Lease.ID,
		Event: llm.Event{
			ID: "embedded-final-event", Type: llm.EventFinal,
			Text: "Hello from a Human worker.",
		},
	})
	if err != nil {
		return err
	}
	if receipt.Decision != llm.WorkerEventACK {
		return fmt.Errorf("worker event was not acknowledged: %+v", receipt)
	}

	var outcome callOutcome
	select {
	case outcome = <-completed:
	case <-ctx.Done():
		return ctx.Err()
	}
	if outcome.err != nil {
		return outcome.err
	}
	_, err = fmt.Fprintf(out, "HumanLLM status=%d content-type=%s body=%s\n",
		outcome.result.StatusCode, outcome.result.ContentType, outcome.result.Body)
	return err
}

func shutdown(runtime framework.Runtime) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return runtime.Shutdown(ctx)
}

func releaseResource[T any](resource framework.Resource[T]) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return resource.Release(ctx)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := run(ctx, os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
