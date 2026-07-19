package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/protect"
)

func TestExtractSubmittedToolResultsKeepsConflictPermanent(t *testing.T) {
	request := Request{
		Model: "human",
		Messages: []Message{
			{Role: RoleTool, Blocks: []Block{{Type: BlockToolResult, ToolCallID: "call-a", Output: "first"}}},
			{Role: RoleTool, Blocks: []Block{{Type: BlockToolResult, ToolCallID: "call-a", Output: "different"}}},
			{Role: RoleTool, Blocks: []Block{{Type: BlockToolResult, ToolCallID: "call-a", Output: "first"}}},
		},
	}
	if _, exists := extractSubmittedToolResults(request)["call-a"]; exists {
		t.Fatal("a third duplicate resurrected a permanently conflicted tool result")
	}
}

func TestStoreRequestPayloadAbsoluteBoundary(t *testing.T) {
	maximum := maximumStoreRequestPayloadBytes
	for _, test := range []struct {
		canonical int64
		body      int64
		limit     int64
		allowed   bool
	}{
		{canonical: maximum, limit: maximum, allowed: true},
		{canonical: maximum - 1, body: 1, limit: maximum, allowed: true},
		{canonical: maximum, body: 1, limit: maximum},
		{canonical: maximum + 1, limit: maximum + 1},
		{canonical: 7, body: 2, limit: 8},
	} {
		if got := storeRequestPayloadAllowed(test.canonical, test.body, test.limit); got != test.allowed {
			t.Fatalf("payload boundary (%d + %d, limit %d) = %v, want %v",
				test.canonical, test.body, test.limit, got, test.allowed)
		}
	}
}

func TestPersistedWorkerToolCheckpointRetainsLargeInteger(t *testing.T) {
	const exact = "9007199254740993"
	checkpoint := encoderCheckpoint{
		Version: 1, Kind: "event", LeaseID: "lease-a", Worker: "worker-a",
		Event: &Event{
			ID: "event-a", Type: EventToolCalls,
			ToolCalls: []ToolCall{{
				ID: "call-a", Name: "calculate", Input: map[string]any{"value": json.Number(exact)},
			}},
		},
		Seed: &EventSeed{EncodedAtUnix: 1},
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var decoded encoderCheckpoint
	if err := decodeCanonicalJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	value := decoded.Event.ToolCalls[0].Input["value"]
	if number, ok := value.(json.Number); !ok || number.String() != exact {
		t.Fatalf("checkpoint integer = %T(%v), want json.Number(%s)", value, value, exact)
	}
	reencoded, err := json.Marshal(decoded)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatalf("checkpoint canonical replay changed: %s / %s (%v)", encoded, reencoded, err)
	}
}

func TestResponseSignalsReleaseCanceledWaitersWithoutLosingOthers(t *testing.T) {
	service := &Service{signals: make(map[StoreRequestKey]*responseSignalState)}
	key := StoreRequestKey{Caller: "caller-a", IdempotencyKey: "request-a"}
	first, releaseFirst := service.responseSignal(key)
	second, releaseSecond := service.responseSignal(key)
	if first != second {
		t.Fatal("concurrent response waiters did not share one generation")
	}
	releaseFirst()
	if len(service.signals) != 1 {
		t.Fatal("releasing one response waiter removed another live waiter")
	}
	service.signalResponse(key)
	select {
	case <-second:
	default:
		t.Fatal("durable response signal did not wake the remaining waiter")
	}
	releaseSecond()
	if len(service.signals) != 0 {
		t.Fatal("completed response signal leaked")
	}

	_, releaseCanceled := service.responseSignal(key)
	releaseCanceled()
	releaseCanceled() // release is deliberately idempotent for defer-friendly adapters.
	if len(service.signals) != 0 {
		t.Fatal("canceled response waiter leaked its signal generation")
	}
}

func TestProtectedServiceRejectsPlainStoredValueDowngrade(t *testing.T) {
	service := &Service{
		deploymentID: "deployment-a", protector: inertProtector{}, readLimitBytes: 1 << 20,
		protectionReadPolicy: ProtectionRequireSealed,
	}
	plain, err := protect.MarshalStoredValue(protect.NewPlainStoredValue([]byte("attacker chosen")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.openPayload(context.Background(), payloadBinding{
		caller: "caller-a", namespace: "request-a", recordType: "request",
		recordID: "request-a", field: "canonical-payload",
	}, plain)
	if err == nil {
		t.Fatal("protected runtime accepted a plain StoredValue downgrade")
	}
	service.protectionReadPolicy = ProtectionAllowPlain
	opened, err := service.openPayload(context.Background(), payloadBinding{
		caller: "caller-a", namespace: "request-a", recordType: "request",
		recordID: "request-a", field: "canonical-payload",
	}, plain)
	if err != nil || string(opened) != "attacker chosen" {
		t.Fatalf("explicit plain migration read = %q, %v", opened, err)
	}
}

func TestConstructorFailureReleasesOwnedProtector(t *testing.T) {
	var released bool
	resource, err := framework.Own[protect.Protector](badDescriptionProtector{}, func(context.Context) error {
		released = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{releaseTimeout: defaultReleaseTimeoutForTest(), storeResource: framework.Borrow[Store](nilStore{})}
	err = service.acquireProtector(context.Background(), resource)
	if err == nil {
		t.Fatal("bad Protector description was accepted")
	}
	if releaseErr := service.releaseResources(context.Background()); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	if !released {
		t.Fatal("owned Protector leaked after Describe/Validate failure")
	}
}

func TestAcquireProtectorRejectsBorrowedTypedNil(t *testing.T) {
	var provider *lyingProtector
	service := &Service{releaseTimeout: defaultReleaseTimeoutForTest()}
	err := service.acquireProtector(
		context.Background(), framework.Borrow[protect.Protector](provider),
	)
	if !errors.Is(err, ErrInvalidServiceConfig) {
		t.Fatalf("borrowed typed-nil Protector error = %v, want ErrInvalidServiceConfig", err)
	}
}

func TestAcquireProtectorRejectsAndReleasesOwnedTypedNil(t *testing.T) {
	var provider *lyingProtector
	released := false
	resource, err := framework.Own[protect.Protector](provider, func(context.Context) error {
		released = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		releaseTimeout: defaultReleaseTimeoutForTest(),
		storeResource:  framework.Borrow[Store](nilStore{}),
	}
	if err := service.acquireProtector(context.Background(), resource); !errors.Is(err, ErrInvalidServiceConfig) {
		t.Fatalf("owned typed-nil Protector error = %v, want ErrInvalidServiceConfig", err)
	}
	if err := service.releaseResources(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Fatal("owned typed-nil Protector was not released")
	}
}

func TestNewServiceEarlyFailureReleasesTransferredResourcesInReverseOrder(t *testing.T) {
	var order []string
	store, err := framework.Own[Store](nilStore{}, func(context.Context) error {
		order = append(order, "store")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	protector, err := framework.Own[protect.Protector](inertProtector{}, func(context.Context) error {
		order = append(order, "protector")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewService(context.Background(), Config{
		DeploymentID: "invalid deployment", Store: store, Protector: protector,
		ReleaseTimeout: -1,
	})
	if !errors.Is(err, ErrInvalidServiceConfig) {
		t.Fatalf("constructor error = %v", err)
	}
	if len(order) != 2 || order[0] != "protector" || order[1] != "store" {
		t.Fatalf("release order = %v, want [protector store]", order)
	}
}

func TestNewServiceEarlyFailureJoinsBothReleaseErrors(t *testing.T) {
	errProtectorRelease := errors.New("protector release failed")
	errStoreRelease := errors.New("store release failed")
	var order []string
	store, err := framework.Own[Store](nilStore{}, func(context.Context) error {
		order = append(order, "store")
		return errStoreRelease
	})
	if err != nil {
		t.Fatal(err)
	}
	protector, err := framework.Own[protect.Protector](inertProtector{}, func(context.Context) error {
		order = append(order, "protector")
		return errProtectorRelease
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewService(context.Background(), Config{
		DeploymentID: "invalid deployment", Store: store, Protector: protector,
		ReleaseTimeout: time.Second,
	})
	for name, target := range map[string]error{
		"constructor":       ErrInvalidServiceConfig,
		"protector release": errProtectorRelease,
		"store release":     errStoreRelease,
	} {
		if !errors.Is(err, target) {
			t.Fatalf("%s error missing from %v", name, err)
		}
	}
	if len(order) != 2 || order[0] != "protector" || order[1] != "store" {
		t.Fatalf("release order = %v, want [protector store]", order)
	}
}

func TestNewServiceStoreValueReleaseRaceIsBoundedAndCleansProtector(t *testing.T) {
	storeReleaseStarted := make(chan struct{})
	allowStoreRelease := make(chan struct{})
	store, err := framework.Own[Store](nilStore{}, func(context.Context) error {
		close(storeReleaseStarted)
		<-allowStoreRelease
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	protectorReleased := make(chan struct{})
	protector, err := framework.Own[protect.Protector](inertProtector{}, func(context.Context) error {
		close(protectorReleased)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	externalReleaseDone := make(chan struct{})
	go func() {
		_ = store.Release(context.Background())
		close(externalReleaseDone)
	}()
	<-storeReleaseStarted

	started := time.Now()
	_, err = NewService(context.Background(), Config{
		DeploymentID: "deployment-a", Store: store, Protector: protector,
		ReleaseTimeout: 10 * time.Millisecond,
	})
	if !errors.Is(err, ErrInvalidServiceConfig) {
		t.Fatalf("constructor error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Store.Value/Release race blocked constructor for %v", elapsed)
	}
	select {
	case <-protectorReleased:
	default:
		t.Fatal("early Store.Value failure leaked the transferred Protector")
	}
	close(allowStoreRelease)
	select {
	case <-externalReleaseDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent Store release did not finish")
	}
}

func TestServiceEnforcesFrozenProtectorLimitsAroundSeal(t *testing.T) {
	description := testProtectorDescription(4, 6)
	provider := &lyingProtector{
		description: description,
		sealed: protect.Envelope{
			Provider: description.Provider, Format: description.Format,
			KeyID: "key-a", KeyVersion: "v1", Data: []byte{1},
		},
	}
	service := &Service{deploymentID: "deployment-a", readLimitBytes: 1 << 20}
	if err := service.acquireProtector(context.Background(), framework.Borrow[protect.Protector](provider)); err != nil {
		t.Fatal(err)
	}
	// The provider may mutate what future Describe calls return; construction
	// freezes the original contract and admission must keep enforcing it.
	provider.description.MaxPlaintextBytes = 1024
	provider.description.MaxEnvelopeBytes = 2048

	if _, err := service.sealPayload(context.Background(), testPayloadBinding(), []byte("12345")); err == nil {
		t.Fatal("core passed over-limit plaintext to Protector.Seal")
	}
	if provider.sealCalls != 0 {
		t.Fatalf("Protector.Seal calls = %d, want 0", provider.sealCalls)
	}

	provider.sealed.Provider = "other"
	if _, err := service.sealPayload(context.Background(), testPayloadBinding(), []byte("1234")); err == nil {
		t.Fatal("core accepted a provider/format contract violation from Seal")
	}
	provider.sealed.Provider = description.Provider
	provider.sealed.Nonce = []byte{1, 2}
	provider.sealed.Data = []byte{1, 2, 3, 4, 5}
	if _, err := service.sealPayload(context.Background(), testPayloadBinding(), []byte("1234")); err == nil {
		t.Fatal("core accepted an envelope exceeding the frozen raw-byte limit")
	}
}

func TestServiceEnforcesFrozenProtectorLimitsAroundOpen(t *testing.T) {
	description := testProtectorDescription(4, 6)
	provider := &lyingProtector{description: description, opened: []byte("12345")}
	service := &Service{
		deploymentID: "deployment-a", protector: provider,
		protectorDescription: description, protectionReadPolicy: ProtectionRequireSealed,
		readLimitBytes: 1 << 20,
	}

	oversize := protect.Envelope{
		Provider: description.Provider, Format: description.Format,
		KeyID: "key-a", KeyVersion: "v1", Nonce: []byte{1, 2}, Data: []byte{1, 2, 3, 4, 5},
	}
	encoded := mustStoredEnvelope(t, oversize)
	if _, err := service.openPayload(context.Background(), testPayloadBinding(), encoded); err == nil {
		t.Fatal("core passed an over-limit envelope to Protector.Open")
	}
	if provider.openCalls != 0 {
		t.Fatalf("Protector.Open calls = %d, want 0", provider.openCalls)
	}

	withinLimit := protect.Envelope{
		Provider: description.Provider, Format: description.Format,
		KeyID: "key-a", KeyVersion: "v1", Nonce: []byte{1}, Data: []byte{1},
	}
	encoded = mustStoredEnvelope(t, withinLimit)
	if _, err := service.openPayload(context.Background(), testPayloadBinding(), encoded); err == nil {
		t.Fatal("core accepted plaintext exceeding the frozen limit from Open")
	}
	if provider.openCalls != 1 {
		t.Fatalf("Protector.Open calls = %d, want 1", provider.openCalls)
	}
}

func testProtectorDescription(plaintext, envelope int64) protect.Description {
	return protect.Description{
		Contract: framework.Contract{ID: protect.ContractID, Major: protect.ContractMajor},
		Provider: "lying", Format: "v1", MaxPlaintextBytes: plaintext, MaxEnvelopeBytes: envelope,
	}
}

func testPayloadBinding() payloadBinding {
	return payloadBinding{
		caller: "caller-a", namespace: "request-a", recordType: "request",
		recordID: "request-a", field: "canonical-payload",
	}
}

func mustStoredEnvelope(t *testing.T, envelope protect.Envelope) []byte {
	t.Helper()
	value, err := protect.NewSealedStoredValue(false, envelope)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protect.MarshalStoredValue(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type lyingProtector struct {
	description protect.Description
	sealed      protect.Envelope
	opened      []byte
	sealCalls   int
	openCalls   int
}

func (provider *lyingProtector) Describe(context.Context) (protect.Description, error) {
	return provider.description, nil
}

func (provider *lyingProtector) Seal(context.Context, protect.Binding, []byte) (protect.Envelope, error) {
	provider.sealCalls++
	return protect.CloneEnvelope(provider.sealed), nil
}

func (provider *lyingProtector) Open(context.Context, protect.Binding, protect.Envelope) ([]byte, error) {
	provider.openCalls++
	return clonePayloadBytes(provider.opened), nil
}

func defaultReleaseTimeoutForTest() time.Duration { return time.Second }

type inertProtector struct{}

func (inertProtector) Describe(context.Context) (protect.Description, error) {
	return protect.Description{
		Contract: framework.Contract{ID: protect.ContractID, Major: protect.ContractMajor},
		Provider: "inert", Format: "v1", MaxPlaintextBytes: 1024, MaxEnvelopeBytes: 2048,
	}, nil
}

func (inertProtector) Seal(context.Context, protect.Binding, []byte) (protect.Envelope, error) {
	return protect.Envelope{}, errors.New("unused")
}

func (inertProtector) Open(context.Context, protect.Binding, protect.Envelope) ([]byte, error) {
	return nil, errors.New("unused")
}

type badDescriptionProtector struct{ inertProtector }

func (badDescriptionProtector) Describe(context.Context) (protect.Description, error) {
	return protect.Description{}, errors.New("KMS unavailable")
}

type nilStore struct{}

func (nilStore) Description() StoreDescription                     { return StoreDescription{} }
func (nilStore) Bind(context.Context, StoreBinding) error          { return nil }
func (nilStore) View(context.Context, func(StoreView) error) error { return nil }
func (nilStore) Update(context.Context, func(StoreTx) error) error { return nil }
