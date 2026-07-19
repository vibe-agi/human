package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	human "github.com/vibe-agi/human"
	"github.com/vibe-agi/human/examples/custom-framework/customprotect"
	"github.com/vibe-agi/human/examples/custom-framework/customstore"
	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
	protectaead "github.com/vibe-agi/human/protect/aead"
)

var (
	errAuthentication   = errors.New("custom transport: authentication failed")
	errTransportClosed  = errors.New("custom transport: runtime is closed")
	errAlreadyStarted   = errors.New("custom transport: already started")
	errEndpointProtocol = errors.New("custom transport: endpoint violated response protocol")
	errEndpointFailure  = errors.New("custom transport: HumanLLM endpoint failed")
)

type auditLog struct {
	out io.Writer
	mu  sync.Mutex
}

func (audit *auditLog) record(message string) {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	_, _ = fmt.Fprintf(audit.out, "audit %s\n", message)
}

// openCustomStore constructs the application's independent physical Store.
// HumanLLM consumes and releases the returned owned Resource.
func openCustomStore(
	ctx context.Context,
	path string,
	audit *auditLog,
) (framework.Resource[llm.Store], error) {
	return customstore.Open(ctx, customstore.Config{
		Path:     path,
		Provider: "example.audited-store",
		Version:  "1",
		Audit:    func(operation string) { audit.record("store." + operation) },
	})
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
	RetryAfter    string
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
		if operationErr := operation.Err(); operationErr != nil {
			return callResult{}, operationErr
		}
		if projected, ok := projectAdmissionError(err); ok {
			return projected, nil
		}
		return callResult{}, errEndpointFailure
	}
	if err := validateAdmission(admission, caller, request.IdempotencyKey); err != nil {
		return callResult{}, err
	}

	result := callResult{Identity: admission.Identity, RequestDigest: admission.RequestDigest}
	page := admission.Response
	state := responseState{}
	for {
		nextState, err := validatePage(page, admission.Identity, admission.RequestDigest, state)
		if err != nil {
			return callResult{}, err
		}
		mergePage(&result, page, state.decisionCommitted)
		state = nextState
		if page.Complete {
			return result, nil
		}
		page, err = runtime.endpoint.WaitResponse(operation, llm.ResponseQuery{
			CallerID: caller, IdempotencyKey: request.IdempotencyKey,
			RequestDigest: admission.RequestDigest, After: state.cursor,
		})
		if err != nil {
			if operationErr := operation.Err(); operationErr != nil {
				return callResult{}, operationErr
			}
			// AdmissionError is meaningful only before a response identity exists.
			// Once admitted, any endpoint failure is deliberately collapsed instead
			// of exposing an implementation error or replacing a durable decision.
			return callResult{}, errEndpointFailure
		}
	}
}

// projectAdmissionError converts the only caller-safe endpoint error into this
// non-HTTP transport's ordinary result shape. It deliberately does not retain,
// wrap, format, or otherwise reveal AdmissionError.Cause. Malformed typed errors
// are treated exactly like unknown endpoint errors by returning ok=false.
func projectAdmissionError(err error) (callResult, bool) {
	var admission *llm.AdmissionError
	if !errors.As(err, &admission) || admission == nil || admission.Failure.Validate() != nil ||
		!safeWireValue(admission.ContentType, 256) ||
		(admission.RetryAfter != "" && !safeWireValue(admission.RetryAfter, 128)) {
		return callResult{}, false
	}
	return callResult{
		StatusCode:  admission.Failure.Status,
		ContentType: admission.ContentType,
		RetryAfter:  admission.RetryAfter,
		Body:        bytes.Clone(admission.Body),
	}, true
}

func validateAdmission(
	admission llm.AdmissionResult,
	caller llm.CallerID,
	key llm.IdempotencyKey,
) error {
	if err := admission.Identity.Validate(); err != nil {
		return fmt.Errorf("%w: invalid admission identity", errEndpointProtocol)
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

type responseState struct {
	seen              bool
	mode              llm.ResponseMode
	cursor            uint64
	decisionCommitted bool
	decision          llm.ResponseDecision
}

func validatePage(
	page llm.ResponsePage,
	identity llm.CompletionIdentity,
	digest llm.StoreDigest,
	state responseState,
) (responseState, error) {
	if page.Identity != identity || page.RequestDigest != digest {
		return responseState{}, fmt.Errorf("%w: response page identity changed", errEndpointProtocol)
	}
	if page.Mode != llm.ResponseAggregate && page.Mode != llm.ResponseStream {
		return responseState{}, fmt.Errorf("%w: response mode is invalid", errEndpointProtocol)
	}
	if state.seen && page.Mode != state.mode {
		return responseState{}, fmt.Errorf("%w: response mode changed", errEndpointProtocol)
	}
	if page.Cursor < state.cursor {
		return responseState{}, fmt.Errorf("%w: response cursor moved backwards", errEndpointProtocol)
	}
	decisionAdvanced := page.DecisionCommitted && !state.decisionCommitted
	if state.seen && !page.Complete && page.Cursor == state.cursor && !decisionAdvanced {
		return responseState{}, fmt.Errorf("%w: incomplete wait made no progress", errEndpointProtocol)
	}
	sequence := state.cursor
	for _, event := range page.Events {
		if event.Sequence <= sequence || event.Sequence > page.Cursor {
			return responseState{}, fmt.Errorf("%w: response event sequence is unordered", errEndpointProtocol)
		}
		sequence = event.Sequence
	}

	next := state
	next.seen = true
	next.mode = page.Mode
	next.cursor = page.Cursor
	if page.DecisionCommitted {
		if err := validateResponseDecision(page.Decision); err != nil {
			return responseState{}, err
		}
		if state.decisionCommitted && !sameResponseDecision(page.Decision, state.decision) {
			return responseState{}, fmt.Errorf("%w: committed response decision changed", errEndpointProtocol)
		}
		if !state.decisionCommitted {
			next.decisionCommitted = true
			next.decision = cloneResponseDecision(page.Decision)
		}
	} else {
		if !emptyResponseDecision(page.Decision) {
			return responseState{}, fmt.Errorf("%w: uncommitted response carries a decision", errEndpointProtocol)
		}
		if state.decisionCommitted {
			return responseState{}, fmt.Errorf("%w: committed response decision disappeared", errEndpointProtocol)
		}
		if len(page.Events) != 0 {
			return responseState{}, fmt.Errorf("%w: response events preceded the decision", errEndpointProtocol)
		}
	}
	if page.Complete && !page.DecisionCommitted {
		return responseState{}, fmt.Errorf("%w: response completed without a decision", errEndpointProtocol)
	}
	switch page.Mode {
	case llm.ResponseAggregate:
		if len(page.Events) != 0 {
			return responseState{}, fmt.Errorf("%w: aggregate response contains stream events", errEndpointProtocol)
		}
		if page.DecisionCommitted && !page.Complete {
			return responseState{}, fmt.Errorf("%w: aggregate decision committed before completion", errEndpointProtocol)
		}
	case llm.ResponseStream:
		if page.DecisionCommitted && len(page.Decision.Body) != 0 {
			return responseState{}, fmt.Errorf("%w: stream decision contains an aggregate body", errEndpointProtocol)
		}
	}
	return next, nil
}

func mergePage(result *callResult, page llm.ResponsePage, decisionAlreadyCommitted bool) {
	if page.DecisionCommitted && !decisionAlreadyCommitted {
		result.StatusCode = page.Decision.StatusCode
		result.ContentType = page.Decision.ContentType
		result.RetryAfter = page.Decision.RetryAfter
		result.Body = bytes.Clone(page.Decision.Body)
	}
	for _, event := range page.Events {
		result.Frames = append(result.Frames, bytes.Clone(event.Data))
	}
}

func validateResponseDecision(decision llm.ResponseDecision) error {
	if decision.StatusCode < 200 || decision.StatusCode > 599 ||
		decision.StatusCode >= 300 && decision.StatusCode < 400 ||
		decision.StatusCode == http.StatusNoContent || decision.StatusCode == http.StatusResetContent ||
		!safeWireValue(decision.ContentType, 256) ||
		(decision.RetryAfter != "" && !safeWireValue(decision.RetryAfter, 128)) {
		return fmt.Errorf("%w: response decision is invalid", errEndpointProtocol)
	}
	return nil
}

func safeWireValue(value string, maximum int) bool {
	if value == "" || len(value) > maximum || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func emptyResponseDecision(decision llm.ResponseDecision) bool {
	return decision.StatusCode == 0 && decision.ContentType == "" &&
		decision.RetryAfter == "" && len(decision.Body) == 0
}

func sameResponseDecision(left, right llm.ResponseDecision) bool {
	return left.StatusCode == right.StatusCode && left.ContentType == right.ContentType &&
		left.RetryAfter == right.RetryAfter && bytes.Equal(left.Body, right.Body)
}

func cloneResponseDecision(decision llm.ResponseDecision) llm.ResponseDecision {
	decision.Body = bytes.Clone(decision.Body)
	return decision
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
	storeDirectory, err := os.MkdirTemp("", "human-custom-framework-")
	if err != nil {
		return fmt.Errorf("create example Store directory: %w", err)
	}
	defer os.RemoveAll(storeDirectory)
	storeResource, err := openCustomStore(ctx, filepath.Join(storeDirectory, "llm.snapshot"), audit)
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
