package llm

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/protect"
)

const (
	defaultAssignmentBuffer   = 16
	defaultWorkerPayloadLimit = int64(16 << 20)
	defaultReadLimitBytes     = int64(64 << 20)
	defaultReleaseTimeout     = 10 * time.Second
	defaultResponseLimit      = 256
	maximumResponseLimit      = 4096
	maximumPersistedStepBytes = int64(64 << 20)
	// Every admitted StoreRequestRecord is bounded by this combined opaque-byte
	// ceiling: canonical request plus aggregate response body. Recovery uses the
	// same invariant instead of a hot-path ReadLimit operators may lower later.
	maximumStoreRequestPayloadBytes = int64(128 << 20)
	maximumRecoveryReadLimitBytes   = maximumStoreRequestPayloadBytes
	workerEnvelopeReserve           = int64(4 << 10)
	maximumWorkspaceRootBytes       = 4096
	assignmentValidationSession     = WorkerSessionID("core-validation")
)

type registeredCodec struct {
	codec         Codec
	description   CodecDescription
	snapshot      CodecSnapshot
	streamType    string
	aggregateType string
	success       int
}

// Service is the transport-neutral HumanLLM correctness core. It owns no HTTP
// listener. Caller transports invoke Admit/ReadResponse/WaitResponse; worker
// transports borrow it through WorkerEndpoint.
type Service struct {
	deploymentID            string
	storeResource           framework.Resource[Store]
	store                   Store
	protectorResource       protect.Resource
	protector               protect.Protector
	protectorDescription    protect.Description
	protectionReadPolicy    ProtectionReadPolicy
	codecs                  map[CodecID]registeredCodec
	clock                   Clock
	ids                     IDSource
	seeds                   SeedSource
	router                  WorkerRouter
	admission               AdmissionPolicy
	toolAuthorizer          ToolAuthorizer
	assignmentBuffer        int
	workerPayloadLimitBytes int64
	readLimitBytes          int64
	releaseTimeout          time.Duration

	mu           sync.Mutex
	closing      bool
	active       int
	drain        chan struct{}
	stopping     chan struct{}
	done         chan struct{}
	runtimeErr   error
	shutdownOnce sync.Once
	connections  map[WorkerID]*serviceWorkerConnection
	assignments  map[WorkerDeliveryID]*assignmentState
	pending      map[WorkerID]map[WorkerDeliveryID]*assignmentState
	signals      map[StoreRequestKey]*responseSignalState
}

var _ framework.Runtime = (*Service)(nil)
var _ WorkerEndpoint = (*Service)(nil)
var _ CallerEndpoint = (*Service)(nil)

type assignmentState struct {
	delivery  WorkerAssignmentDelivery
	request   StoreRequestKey
	pending   bool
	createdAt time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type defaultIDSource struct{}

func (defaultIDSource) NewID(_ context.Context, kind IDKind) (string, error) {
	prefix := map[IDKind]string{IDTask: "task_", IDRequest: "req_", IDResponse: "resp_", IDLease: "lease_"}[kind]
	if prefix == "" {
		return "", fmt.Errorf("unsupported HumanLLM id kind %q", kind)
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("allocate HumanLLM %s id: %w", kind, err)
	}
	return prefix + hex.EncodeToString(random), nil
}

type clockSeedSource struct{ clock Clock }

func (source clockSeedSource) SessionSeed(context.Context, SeedContext) (SessionSeed, error) {
	now, err := checkedTime(source.clock)
	if err != nil {
		return SessionSeed{}, err
	}
	return SessionSeed{CreatedAtUnix: now.Unix()}, nil
}

func (source clockSeedSource) EventSeed(context.Context, EventSeedContext) (EventSeed, error) {
	now, err := checkedTime(source.clock)
	if err != nil {
		return EventSeed{}, err
	}
	return EventSeed{EncodedAtUnix: now.Unix()}, nil
}

// NewService validates every injected contract, recovers durable in-flight
// responses, and returns an active transport-neutral core. ctx bounds only
// construction and recovery.
func NewService(ctx context.Context, config Config) (_ *Service, resultErr error) {
	cleanupTimeout := config.ReleaseTimeout
	if cleanupTimeout < time.Millisecond || cleanupTimeout > 5*time.Minute {
		cleanupTimeout = defaultReleaseTimeout
	}
	service := &Service{
		deploymentID: config.DeploymentID, storeResource: config.Store,
		protectorResource:    config.Protector,
		protectionReadPolicy: config.ProtectionReadPolicy,
		clock:                config.Clock, ids: config.IDs, seeds: config.Seeds,
		router: config.Router, admission: config.Admission,
		toolAuthorizer:   config.ToolAuthorizer,
		assignmentBuffer: config.AssignmentBuffer, workerPayloadLimitBytes: config.WorkerPayloadLimitBytes,
		readLimitBytes: config.ReadLimitBytes,
		releaseTimeout: cleanupTimeout,
		codecs:         make(map[CodecID]registeredCodec), drain: make(chan struct{}),
		stopping: make(chan struct{}), done: make(chan struct{}),
		connections: make(map[WorkerID]*serviceWorkerConnection),
		assignments: make(map[WorkerDeliveryID]*assignmentState),
		pending:     make(map[WorkerID]map[WorkerDeliveryID]*assignmentState),
		signals:     make(map[StoreRequestKey]*responseSignalState),
	}
	cleanup := true
	defer func() {
		if cleanup {
			// Ownership transfers at the constructor boundary, not after Value or
			// validation succeeds. Release in reverse dependency-acquisition order.
			resultErr = errors.Join(resultErr, service.releaseResources(context.Background()))
		}
	}()
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidServiceConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !workerStableKeyPattern.MatchString(config.DeploymentID) {
		return nil, fmt.Errorf("%w: deployment id must be a stable key", ErrInvalidServiceConfig)
	}
	releaseTimeout := config.ReleaseTimeout
	if releaseTimeout == 0 {
		releaseTimeout = defaultReleaseTimeout
	}
	if releaseTimeout < time.Millisecond || releaseTimeout > 5*time.Minute {
		return nil, fmt.Errorf("%w: release timeout must be 1ms..5m", ErrInvalidServiceConfig)
	}
	service.releaseTimeout = releaseTimeout
	store, err := config.Store.Value()
	if err != nil {
		return nil, fmt.Errorf("%w: acquire Store: %v", ErrInvalidServiceConfig, err)
	}
	service.store = store
	if nilInterface(store) {
		return nil, fmt.Errorf("%w: Store is required", ErrInvalidServiceConfig)
	}
	if err := store.Description().Validate(); err != nil {
		return nil, fmt.Errorf("%w: Store: %v", ErrInvalidServiceConfig, err)
	}
	if config.Clock == nil {
		service.clock = systemClock{}
	} else if nilInterface(config.Clock) {
		return nil, fmt.Errorf("%w: Clock is a typed nil", ErrInvalidServiceConfig)
	}
	if _, err := checkedTime(service.clock); err != nil {
		return nil, fmt.Errorf("%w: Clock: %v", ErrInvalidServiceConfig, err)
	}
	if config.IDs == nil {
		service.ids = defaultIDSource{}
	} else if nilInterface(config.IDs) {
		return nil, fmt.Errorf("%w: IDs is a typed nil", ErrInvalidServiceConfig)
	}
	if config.Seeds == nil {
		service.seeds = clockSeedSource{clock: service.clock}
	} else if nilInterface(config.Seeds) {
		return nil, fmt.Errorf("%w: Seeds is a typed nil", ErrInvalidServiceConfig)
	}
	if nilInterface(config.Router) && config.Router != nil {
		return nil, fmt.Errorf("%w: Router is a typed nil", ErrInvalidServiceConfig)
	}
	if nilInterface(config.Admission) && config.Admission != nil {
		return nil, fmt.Errorf("%w: Admission is a typed nil", ErrInvalidServiceConfig)
	}
	if nilInterface(config.ToolAuthorizer) && config.ToolAuthorizer != nil {
		return nil, fmt.Errorf("%w: ToolAuthorizer is a typed nil", ErrInvalidServiceConfig)
	}
	if service.assignmentBuffer == 0 {
		service.assignmentBuffer = defaultAssignmentBuffer
	}
	if service.assignmentBuffer < 1 || service.assignmentBuffer > 4096 {
		return nil, fmt.Errorf("%w: assignment buffer must be 1..4096", ErrInvalidServiceConfig)
	}
	if service.workerPayloadLimitBytes == 0 {
		service.workerPayloadLimitBytes = defaultWorkerPayloadLimit
	}
	if service.workerPayloadLimitBytes < 8<<10 || service.workerPayloadLimitBytes > 64<<20 {
		return nil, fmt.Errorf("%w: worker payload limit must be 8KiB..64MiB", ErrInvalidServiceConfig)
	}
	if service.readLimitBytes == 0 {
		service.readLimitBytes = defaultReadLimitBytes
	}
	if service.readLimitBytes < 1 || service.readLimitBytes > maximumStoreRequestPayloadBytes {
		return nil, fmt.Errorf("%w: read limit must be 1..128MiB", ErrInvalidServiceConfig)
	}
	if err := service.registerCodecs(config.Codecs); err != nil {
		return nil, err
	}
	if err := service.acquireProtector(ctx, config.Protector); err != nil {
		return nil, err
	}
	if service.protectionReadPolicy == "" {
		service.protectionReadPolicy = ProtectionRequireSealed
	}
	if service.protectionReadPolicy != ProtectionRequireSealed &&
		service.protectionReadPolicy != ProtectionAllowPlain {
		return nil, fmt.Errorf("%w: invalid protection read policy %q", ErrInvalidServiceConfig, service.protectionReadPolicy)
	}
	// Bind only after every side-effect-free composition check succeeds. From
	// this point onward the selected deployment permanently owns the Store
	// namespace, even if recovery later exposes corrupt durable state.
	if err := service.bindStore(ctx); err != nil {
		return nil, fmt.Errorf("%w: bind Store: %w", ErrInvalidServiceConfig, err)
	}
	if err := service.recover(ctx); err != nil {
		return nil, fmt.Errorf("recover HumanLLM Service: %w", err)
	}
	cleanup = false
	return service, nil
}

func storeRequestPayloadAllowed(canonicalBytes, decisionBodyBytes, runtimeLimit int64) bool {
	limit := min(runtimeLimit, maximumStoreRequestPayloadBytes)
	return canonicalBytes >= 0 && decisionBodyBytes >= 0 && limit > 0 &&
		canonicalBytes <= limit && decisionBodyBytes <= limit-canonicalBytes
}

func (service *Service) bindStore(ctx context.Context) error {
	binding := StoreBinding{DeploymentID: service.deploymentID}
	err := service.store.Bind(ctx, binding)
	if !errors.Is(err, ErrStoreCommitUnknown) {
		return err
	}
	// The first Bind may have committed before its acknowledgement was lost.
	// Exact rebinding is the Store contract's reconciliation operation.
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return service.store.Bind(reconcileCtx, binding)
}

func (service *Service) registerCodecs(registrations []CodecRegistration) error {
	if len(registrations) == 0 {
		return fmt.Errorf("%w: at least one Codec is required", ErrInvalidServiceConfig)
	}
	for _, registration := range registrations {
		description, err := ValidateCodec(registration.Codec)
		if err != nil {
			return fmt.Errorf("%w: Codec: %v", ErrInvalidServiceConfig, err)
		}
		if _, duplicate := service.codecs[description.ID]; duplicate {
			return fmt.Errorf("%w: duplicate Codec %q", ErrInvalidServiceConfig, description.ID)
		}
		if description.Limits.MaxStreamFrameBytes > maximumPersistedStepBytes ||
			description.Limits.MaxAggregateBytes > maximumPersistedStepBytes {
			return fmt.Errorf(
				"%w: Codec %q output exceeds the 64MiB durable payload boundary",
				ErrInvalidServiceConfig, description.ID,
			)
		}
		if !validateServiceContentType(registration.StreamContentType) ||
			!validateServiceContentType(registration.AggregateContentType) {
			return fmt.Errorf("%w: Codec %q has invalid content type", ErrInvalidServiceConfig, description.ID)
		}
		status := registration.SuccessStatus
		if status == 0 {
			status = 200
		}
		if status < 200 || status > 299 || status == 204 || status == 205 {
			return fmt.Errorf(
				"%w: Codec %q success status must be a body-carrying 2xx",
				ErrInvalidServiceConfig, description.ID,
			)
		}
		snapshot, err := NewCodecSnapshot(description)
		if err != nil {
			return fmt.Errorf("%w: Codec %q snapshot: %v", ErrInvalidServiceConfig, description.ID, err)
		}
		service.codecs[description.ID] = registeredCodec{
			codec: registration.Codec, description: description, snapshot: snapshot,
			streamType:    registration.StreamContentType,
			aggregateType: registration.AggregateContentType, success: status,
		}
	}
	return nil
}

func (service *Service) acquireProtector(ctx context.Context, resource protect.Resource) error {
	// Retain ownership before any fallible Describe/Validate call so constructor
	// cleanup cannot leak an owned KMS/keyring resource.
	service.protectorResource = resource
	value, err := resource.Value()
	if err != nil {
		return fmt.Errorf("%w: acquire Protector: %v", ErrInvalidServiceConfig, err)
	}
	if value == nil {
		if resource.Owned() {
			return fmt.Errorf("%w: owned Protector contains nil", ErrInvalidServiceConfig)
		}
		return nil
	}
	if nilInterface(value) {
		return fmt.Errorf("%w: Protector contains a typed nil", ErrInvalidServiceConfig)
	}
	description, err := value.Describe(ctx)
	if err != nil {
		return fmt.Errorf("%w: describe Protector: %v", ErrInvalidServiceConfig, err)
	}
	if err := protect.ValidateDescription(description); err != nil {
		return fmt.Errorf("%w: Protector: %v", ErrInvalidServiceConfig, err)
	}
	service.protector = value
	service.protectorDescription = description
	return nil
}

func (service *Service) Done() <-chan struct{} { return service.done }

func (service *Service) Err() error {
	select {
	case <-service.done:
		service.mu.Lock()
		defer service.mu.Unlock()
		return service.runtimeErr
	default:
		return nil
	}
}

func (service *Service) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("llm: shutdown context is required")
	}
	service.shutdownOnce.Do(func() {
		service.mu.Lock()
		service.closing = true
		close(service.stopping)
		connections := make([]*serviceWorkerConnection, 0, len(service.connections))
		for _, connection := range service.connections {
			connections = append(connections, connection)
		}
		if service.active == 0 {
			close(service.drain)
		}
		service.mu.Unlock()
		for _, connection := range connections {
			connection.stop(nil)
		}
		go service.finishShutdown(connections)
	})
	select {
	case <-service.done:
		return service.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (service *Service) finishShutdown(connections []*serviceWorkerConnection) {
	<-service.drain
	for _, connection := range connections {
		<-connection.Done()
	}
	err := service.releaseResources(context.Background())
	service.mu.Lock()
	service.runtimeErr = err
	service.mu.Unlock()
	close(service.done)
}

func (service *Service) releaseResources(ctx context.Context) error {
	return errors.Join(
		service.releaseResource(ctx, service.protectorResource.Release),
		service.releaseResource(ctx, service.storeResource.Release),
	)
}

func (service *Service) releaseResource(ctx context.Context, release framework.ReleaseFunc) error {
	if ctx == nil {
		ctx = context.Background()
	}
	releaseCtx, cancel := context.WithTimeout(ctx, service.releaseTimeout)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- release(releaseCtx) }()
	select {
	case err := <-result:
		return err
	case <-releaseCtx.Done():
		return releaseCtx.Err()
	}
}

func (service *Service) beginOperation() (func(), error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closing {
		return nil, ErrServiceClosed
	}
	service.active++
	return func() {
		service.mu.Lock()
		service.active--
		if service.closing && service.active == 0 {
			select {
			case <-service.drain:
			default:
				close(service.drain)
			}
		}
		service.mu.Unlock()
	}, nil
}

func checkedTime(clock Clock) (time.Time, error) {
	if clock == nil {
		return time.Time{}, errors.New("clock is nil")
	}
	now := clock.Now()
	if now.IsZero() {
		return time.Time{}, errors.New("clock returned zero time")
	}
	return time.Unix(0, now.UnixNano()).UTC(), nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func stableDigest(value any) (StoreDigest, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return StoreDigest(hex.EncodeToString(digest[:])), nil
}

func normalizeCanonicalJSON[T any](value T) (T, error) {
	var normalized T
	encoded, err := json.Marshal(value)
	if err != nil {
		return normalized, err
	}
	if err := decodeCanonicalJSON(encoded, &normalized); err != nil {
		return normalized, err
	}
	return normalized, nil
}

func normalizeSessionSeed(seed SessionSeed) (SessionSeed, error) {
	normalized := seed
	normalized.Entropy = bytes.Clone(seed.Entropy)
	opaque, err := normalizeSeedOpaque(seed.Opaque)
	if err != nil {
		return SessionSeed{}, err
	}
	normalized.Opaque = opaque
	return normalized, nil
}

func normalizeEventSeed(seed EventSeed) (EventSeed, error) {
	normalized := seed
	normalized.Entropy = bytes.Clone(seed.Entropy)
	opaque, err := normalizeSeedOpaque(seed.Opaque)
	if err != nil {
		return EventSeed{}, err
	}
	normalized.Opaque = opaque
	return normalized, nil
}

func normalizeSeedOpaque(opaque json.RawMessage) (json.RawMessage, error) {
	if len(opaque) == 0 {
		return nil, nil
	}
	var value any
	if err := decodeCanonicalJSON(opaque, &value); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}

// decodeCanonicalJSON is the one decoder for persisted or hook-supplied JSON
// values. UseNumber is part of the canonical contract: converting an integer
// through float64 would change its digest and make an otherwise valid request
// unrecoverable after restart.
func decodeCanonicalJSON(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("canonical JSON has trailing data")
	}
	return nil
}

func cloneContractFeatures(input map[framework.Feature]uint16) map[framework.Feature]uint16 {
	if input == nil {
		return nil
	}
	cloned := make(map[framework.Feature]uint16, len(input))
	for feature, version := range input {
		cloned[feature] = version
	}
	return cloned
}

func cloneCodecDescription(input CodecDescription) CodecDescription {
	cloned := input
	cloned.Contract.Features = cloneContractFeatures(input.Contract.Features)
	return cloned
}

func cloneCodecSnapshot(input CodecSnapshot) CodecSnapshot {
	cloned := input
	cloned.Contract.Features = cloneContractFeatures(input.Contract.Features)
	return cloned
}

func cloneStoreTaskRecord(input StoreTaskRecord) StoreTaskRecord {
	cloned := input
	cloned.Codec = cloneCodecSnapshot(input.Codec)
	return cloned
}

func stableAssignmentDeliveryID(requestID string, lease WorkerLeaseID) WorkerDeliveryID {
	digest := sha256.Sum256([]byte(requestID + "\x00" + string(lease)))
	return WorkerDeliveryID("assign_" + hex.EncodeToString(digest[:16]))
}

func (service *Service) connectedWorkers() []WorkerID {
	service.mu.Lock()
	defer service.mu.Unlock()
	workers := make([]WorkerID, 0, len(service.connections))
	for worker := range service.connections {
		workers = append(workers, worker)
	}
	sort.Slice(workers, func(left, right int) bool { return workers[left] < workers[right] })
	return workers
}

func parseRetryAfter(duration time.Duration) string {
	if duration <= 0 {
		return ""
	}
	seconds := (duration + time.Second - 1) / time.Second
	return strconv.FormatInt(int64(seconds), 10)
}

func validGeneratedID(kind IDKind, value string) error {
	if !workerStableKeyPattern.MatchString(value) {
		return fmt.Errorf("IDSource returned invalid %s id %q", kind, value)
	}
	return nil
}

func canonicalRequestDigest(snapshot CodecSnapshot, request Request) (StoreDigest, []byte, error) {
	canonical, err := json.Marshal(request)
	if err != nil {
		return "", nil, fmt.Errorf("encode canonical request: %w", err)
	}
	digest, err := stableDigest(struct {
		Codec   CodecSnapshot   `json:"codec"`
		Request json.RawMessage `json:"request"`
	}{Codec: snapshot, Request: canonical})
	return digest, canonical, err
}

func checkPersistedFrameStep(frames [][]byte) error {
	var total int64
	for _, frame := range frames {
		if int64(len(frame)) > maximumPersistedStepBytes-total {
			return fmt.Errorf("%w: one encoder step exceeds the 64MiB durable payload boundary", ErrInvalidCodecContract)
		}
		total += int64(len(frame))
	}
	return nil
}

func normalizeTaskContext(input TaskContext) TaskContext {
	if input.CapabilityTier == "" {
		input.CapabilityTier = TierChat
	}
	return input
}

func sameTaskContext(record StoreTaskRecord, input TaskContext) bool {
	return record.WorkspaceKey == input.WorkspaceKey && record.CapabilityTier == input.CapabilityTier &&
		record.HarnessID == input.HarnessID && record.HarnessVersion == input.HarnessVersion &&
		record.HarnessSessionID == input.HarnessSessionID && record.WorkspaceRoot == input.WorkspaceRoot &&
		record.ExecAllowed == input.ExecAllowed
}
