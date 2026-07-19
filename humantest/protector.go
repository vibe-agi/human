package humantest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/protect"
)

// ProtectorFixture describes one complete key-rotation lifecycle for the
// provider-neutral Protector conformance suite.
//
// Initial writes with an old key version. Reopen reconstructs an independent
// Protector with the same readable versions, as after a process restart.
// Rotate reconstructs an independent Protector which can still read Initial
// envelopes but writes with a different active key reference. CurrentOnly
// reconstructs one which can read the rotated envelope but no longer has the
// Initial key. Every callback must return a fresh Resource.
//
// Resources may be owned or borrowed. TestProtector releases every Resource;
// framework.Resource guarantees that borrowed dependencies remain caller-owned
// while owned dependencies are released at most once. A factory returning a
// borrowed dependency must arrange its own cleanup through testing.TB.Cleanup.
// DiagnosticSecrets optionally lists sensitive configuration strings which
// must never occur in Description values or operation errors.
type ProtectorFixture struct {
	Initial           protect.Resource
	Reopen            func(context.Context) (protect.Resource, error)
	Rotate            func(context.Context) (protect.Resource, error)
	CurrentOnly       func(context.Context) (protect.Resource, error)
	DiagnosticSecrets []string
}

// ProtectorFactory creates an independent rotation lifecycle for each
// conformance subtest. The context bounds construction only and must not be
// retained by the Protector.
type ProtectorFactory func(context.Context, testing.TB) (ProtectorFixture, error)

// TestProtector runs the public black-box conformance suite for protect.Protector.
// It verifies contract negotiation, explicit ownership, exact nil/empty and byte
// ownership, binding and envelope authentication, key-version rotation and
// restart reconstruction, cancellation, diagnostic confidentiality, and
// concurrent use.
//
// The suite invokes Protector operations directly. Human cores must likewise do
// all Protector/KMS work before or after Store View/Update callbacks, never from
// inside one: a conforming provider is allowed to block on network or hardware.
func TestProtector(t *testing.T, factory ProtectorFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("Protector conformance factory is nil")
	}

	tests := []struct {
		name string
		run  func(context.Context, *testing.T, ProtectorFixture, protect.Protector)
	}{
		{"description_and_resource_lifecycle", testProtectorDescriptionAndLifecycle},
		{"round_trip_and_byte_ownership", testProtectorRoundTripAndBytes},
		{"nil_and_empty_are_distinct", testProtectorNilAndEmpty},
		{"binding_authenticates_every_field", testProtectorBinding},
		{"envelope_tamper_and_metadata", testProtectorEnvelopeTamper},
		{"reopen_rotation_and_retirement", testProtectorRotation},
		{"context_and_diagnostic_confidentiality", testProtectorContextAndDiagnostics},
		{"concurrent_seal_and_open", testProtectorConcurrent},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			t.Cleanup(cancel)
			fixture, err := factory(ctx, t)
			if err != nil {
				t.Fatalf("open Protector fixture: %v", err)
			}
			if fixture.Reopen == nil || fixture.Rotate == nil || fixture.CurrentOnly == nil {
				t.Fatal("Protector fixture reconstruction callbacks must be non-nil")
			}
			protector := acquireProtector(t, fixture.Initial)
			test.run(ctx, t, fixture, protector)
		})
	}
}

func acquireProtector(t *testing.T, resource protect.Resource) protect.Protector {
	t.Helper()
	protector, err := resource.Value()
	if err != nil {
		t.Fatalf("acquire Protector Resource: %v", err)
	}
	if nilInterface(protector) {
		t.Fatal("Protector Resource contains a nil Protector")
	}
	t.Cleanup(func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := resource.Release(releaseCtx); err != nil {
			t.Errorf("release Protector Resource: %v", err)
		}
	})
	return protector
}

func reconstructProtector(
	ctx context.Context,
	t *testing.T,
	label string,
	open func(context.Context) (protect.Resource, error),
) (protect.Resource, protect.Protector) {
	t.Helper()
	resource, err := open(ctx)
	if err != nil {
		t.Fatalf("open %s Protector: %v", label, err)
	}
	return resource, acquireProtector(t, resource)
}

func testProtectorDescriptionAndLifecycle(ctx context.Context, t *testing.T, fixture ProtectorFixture, protector protect.Protector) {
	t.Helper()
	first, err := protector.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if err := protect.ValidateDescription(first); err != nil {
		t.Fatalf("Description does not satisfy Protector contract: %v", err)
	}
	second, err := protector.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe again: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Description changed at runtime:\nfirst:  %#v\nsecond: %#v", first, second)
	}

	owned := fixture.Initial.Owned()
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fixture.Initial.Release(releaseCtx); err != nil {
		t.Fatalf("release Resource: %v", err)
	}
	if err := fixture.Initial.Release(releaseCtx); err != nil {
		t.Fatalf("idempotent Resource release: %v", err)
	}
	_, valueErr := fixture.Initial.Value()
	if owned && !errors.Is(valueErr, framework.ErrResourceReleased) {
		t.Fatalf("owned Resource Value after release error = %v, want ErrResourceReleased", valueErr)
	}
	if !owned && valueErr != nil {
		t.Fatalf("borrowed Resource was released: %v", valueErr)
	}
}

func testProtectorRoundTripAndBytes(ctx context.Context, t *testing.T, _ ProtectorFixture, protector protect.Protector) {
	t.Helper()
	description := mustProtectorDescription(ctx, t, protector)
	size := min(int64(64), description.MaxPlaintextBytes)
	plaintext := bytes.Repeat([]byte{0x5a}, int(size))
	want := append([]byte(nil), plaintext...)
	binding := protectorBinding()
	envelope, err := protector.Seal(ctx, binding, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := protect.ValidateEnvelope(envelope); err != nil {
		t.Fatalf("Seal returned invalid Envelope: %v", err)
	}
	if envelope.Provider != description.Provider || envelope.Format != description.Format {
		t.Fatalf("Envelope identity = %q/%q, Description = %q/%q",
			envelope.Provider, envelope.Format, description.Provider, description.Format)
	}
	stored, err := protect.MarshalEnvelope(envelope)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	envelope, err = protect.UnmarshalEnvelopeLimited(stored, int64(len(stored)))
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	clear(plaintext)
	opened, err := protector.Open(ctx, binding, envelope)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(opened, want) {
		t.Fatalf("Open payload = %x, want %x", opened, want)
	}

	// Mutating caller-owned input and output buffers after a call must not alter
	// either the retained envelope or a later Open result.
	clear(opened)
	again, err := protector.Open(ctx, binding, envelope)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatalf("Open after output mutation = %x / %v, want %x", again, err, want)
	}
	cloned := protect.CloneEnvelope(envelope)
	envelope.Data[0] ^= 0xff
	if len(envelope.Nonce) > 0 {
		envelope.Nonce[0] ^= 0xff
	}
	again, err = protector.Open(ctx, binding, cloned)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatalf("Open after Envelope mutation = %x / %v, want %x", again, err, want)
	}
}

func testProtectorNilAndEmpty(ctx context.Context, t *testing.T, _ ProtectorFixture, protector protect.Protector) {
	t.Helper()
	binding := protectorBinding()
	nilEnvelope, err := protector.Seal(ctx, binding, nil)
	if err != nil {
		t.Fatalf("Seal nil: %v", err)
	}
	emptyEnvelope, err := protector.Seal(ctx, binding, []byte{})
	if err != nil {
		t.Fatalf("Seal non-nil empty: %v", err)
	}
	openedNil, err := protector.Open(ctx, binding, nilEnvelope)
	if err != nil {
		t.Fatalf("Open nil: %v", err)
	}
	if openedNil != nil {
		t.Fatalf("Open nil returned non-nil %#v", openedNil)
	}
	openedEmpty, err := protector.Open(ctx, binding, emptyEnvelope)
	if err != nil {
		t.Fatalf("Open empty: %v", err)
	}
	if openedEmpty == nil || len(openedEmpty) != 0 {
		t.Fatalf("Open non-nil empty returned %#v", openedEmpty)
	}
}

func testProtectorBinding(ctx context.Context, t *testing.T, _ ProtectorFixture, protector protect.Protector) {
	t.Helper()
	binding := protectorBinding()
	envelope, err := protector.Seal(ctx, binding, []byte{'b'})
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name   string
		mutate func(*protect.Binding)
	}{
		{"component", func(value *protect.Binding) { value.Component = "human-agent" }},
		{"purpose", func(value *protect.Binding) { value.Purpose = "artifact-at-rest" }},
		{"authority", func(value *protect.Binding) { value.Authority = "tenant-b" }},
		{"namespace", func(value *protect.Binding) { value.Namespace = "workspace-b" }},
		{"record_type", func(value *protect.Binding) { value.RecordType = "message" }},
		{"record_id", func(value *protect.Binding) { value.RecordID = "request-b" }},
		{"field", func(value *protect.Binding) { value.Field = "response" }},
		{"schema", func(value *protect.Binding) { value.Schema++ }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := binding
			mutation.mutate(&changed)
			if _, err := protector.Open(ctx, changed, envelope); !errors.Is(err, protect.ErrAuthentication) {
				t.Fatalf("Open with changed %s error = %v, want ErrAuthentication", mutation.name, err)
			}
		})
	}
}

func testProtectorEnvelopeTamper(ctx context.Context, t *testing.T, _ ProtectorFixture, protector protect.Protector) {
	t.Helper()
	binding := protectorBinding()
	envelope, err := protector.Seal(ctx, binding, []byte{'c'})
	if err != nil {
		t.Fatal(err)
	}
	dataTamper := protect.CloneEnvelope(envelope)
	dataTamper.Data[len(dataTamper.Data)/2] ^= 0x80
	if _, err := protector.Open(ctx, binding, dataTamper); !errors.Is(err, protect.ErrAuthentication) {
		t.Fatalf("ciphertext tamper error = %v, want ErrAuthentication", err)
	}
	if len(envelope.Nonce) > 0 {
		nonceTamper := protect.CloneEnvelope(envelope)
		nonceTamper.Nonce[0] ^= 0x80
		if _, err := protector.Open(ctx, binding, nonceTamper); !errors.Is(err, protect.ErrAuthentication) {
			t.Fatalf("nonce tamper error = %v, want ErrAuthentication", err)
		}
	}

	providerTamper := protect.CloneEnvelope(envelope)
	providerTamper.Provider = differentToken(envelope.Provider, "humantest-provider")
	if _, err := protector.Open(ctx, binding, providerTamper); err == nil {
		t.Fatal("Open accepted a different provider")
	}
	formatTamper := protect.CloneEnvelope(envelope)
	formatTamper.Format = differentToken(envelope.Format, "humantest-format")
	if _, err := protector.Open(ctx, binding, formatTamper); err == nil {
		t.Fatal("Open accepted a different format")
	}
	keyTamper := protect.CloneEnvelope(envelope)
	keyTamper.KeyVersion = differentToken(envelope.KeyVersion, "humantest-missing-version")
	if _, err := protector.Open(ctx, binding, keyTamper); err == nil {
		t.Fatal("Open accepted a different key version")
	}
}

func testProtectorRotation(ctx context.Context, t *testing.T, fixture ProtectorFixture, initial protect.Protector) {
	t.Helper()
	binding := protectorBinding()
	oldEnvelope, err := initial.Seal(ctx, binding, []byte{'o'})
	if err != nil {
		t.Fatalf("Seal with initial key: %v", err)
	}
	stored, err := protect.MarshalEnvelope(oldEnvelope)
	if err != nil {
		t.Fatalf("persist initial Envelope: %v", err)
	}
	oldEnvelope, err = protect.UnmarshalEnvelope(stored)
	if err != nil {
		t.Fatalf("restore initial Envelope: %v", err)
	}
	// Close an owned initial adapter before reconstruction to model a process
	// lifetime ending. Borrowed resources intentionally remain caller-owned.
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := fixture.Initial.Release(releaseCtx); err != nil {
		cancel()
		t.Fatalf("release initial Protector: %v", err)
	}
	cancel()

	_, reopened := reconstructProtector(ctx, t, "reopened", fixture.Reopen)
	if payload, err := reopened.Open(ctx, binding, oldEnvelope); err != nil || !bytes.Equal(payload, []byte{'o'}) {
		t.Fatalf("reopened Open = %q / %v", payload, err)
	}

	_, rotated := reconstructProtector(ctx, t, "rotated", fixture.Rotate)
	if _, err := rotated.Open(ctx, binding, oldEnvelope); err != nil {
		t.Fatalf("rotated Protector cannot read old key: %v", err)
	}
	newEnvelope, err := rotated.Seal(ctx, binding, []byte{'n'})
	if err != nil {
		t.Fatalf("Seal with rotated Protector: %v", err)
	}
	if oldEnvelope.KeyID == newEnvelope.KeyID && oldEnvelope.KeyVersion == newEnvelope.KeyVersion {
		t.Fatal("rotation did not change key ID or key version")
	}
	if oldEnvelope.Provider != newEnvelope.Provider || oldEnvelope.Format != newEnvelope.Format {
		t.Fatal("rotation changed provider or wire format")
	}
	retainedEnvelope := newEnvelope
	if rewrapper, ok := rotated.(protect.Rewrapper); ok {
		retainedEnvelope, err = rewrapper.Rewrap(ctx, binding, oldEnvelope)
		if err != nil {
			t.Fatalf("Rewrap old Envelope: %v", err)
		}
		if retainedEnvelope.KeyID != newEnvelope.KeyID || retainedEnvelope.KeyVersion != newEnvelope.KeyVersion {
			t.Fatal("Rewrap did not select the rotated active key")
		}
	}

	_, currentOnly := reconstructProtector(ctx, t, "current-only", fixture.CurrentOnly)
	if payload, err := currentOnly.Open(ctx, binding, retainedEnvelope); err != nil || len(payload) == 0 {
		t.Fatalf("current-only Open rotated Envelope = %q / %v", payload, err)
	}
	if _, err := currentOnly.Open(ctx, binding, oldEnvelope); err == nil {
		t.Fatal("current-only Protector unexpectedly retained the old key")
	}
}

func testProtectorContextAndDiagnostics(ctx context.Context, t *testing.T, fixture ProtectorFixture, protector protect.Protector) {
	t.Helper()
	description := mustProtectorDescription(ctx, t, protector)
	canary := "humantest-plaintext-must-not-appear-in-diagnostics"
	checkCanary := int64(len(canary)) <= description.MaxPlaintextBytes
	if int64(len(canary)) > description.MaxPlaintextBytes {
		canary = "x"
	}
	binding := protectorBinding()
	envelope, err := protector.Seal(ctx, binding, []byte(canary))
	if err != nil {
		t.Fatal(err)
	}
	wrong := binding
	wrong.RecordID = "diagnostic-mismatch"
	_, operationErr := protector.Open(ctx, wrong, envelope)
	if operationErr == nil {
		t.Fatal("mismatched binding unexpectedly opened")
	}
	diagnostics := fmt.Sprintf("%#v %v", description, operationErr)
	forbiddenDiagnostics := append([]string(nil), fixture.DiagnosticSecrets...)
	if checkCanary {
		forbiddenDiagnostics = append(forbiddenDiagnostics, canary)
	}
	for _, forbidden := range forbiddenDiagnostics {
		if forbidden != "" && strings.Contains(diagnostics, forbidden) {
			t.Fatalf("Description or error disclosed protected material %q", forbidden)
		}
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := protector.Describe(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Describe cancelled error = %v, want context.Canceled", err)
	}
	if _, err := protector.Seal(cancelled, binding, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Seal cancelled error = %v, want context.Canceled", err)
	}
	if _, err := protector.Open(cancelled, binding, envelope); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancelled error = %v, want context.Canceled", err)
	}
}

func testProtectorConcurrent(ctx context.Context, t *testing.T, _ ProtectorFixture, protector protect.Protector) {
	t.Helper()
	description := mustProtectorDescription(ctx, t, protector)
	payloadBytes := min(int64(32), description.MaxPlaintextBytes)
	const workers = 32
	var wait sync.WaitGroup
	errorsFound := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			binding := protectorBinding()
			binding.RecordID = fmt.Sprintf("concurrent-%d", worker)
			payload := bytes.Repeat([]byte{byte(worker + 1)}, int(payloadBytes))
			for iteration := 0; iteration < 25; iteration++ {
				if _, err := protector.Describe(ctx); err != nil {
					errorsFound <- fmt.Errorf("worker %d Describe: %w", worker, err)
					return
				}
				envelope, err := protector.Seal(ctx, binding, payload)
				if err != nil {
					errorsFound <- fmt.Errorf("worker %d Seal: %w", worker, err)
					return
				}
				opened, err := protector.Open(ctx, binding, envelope)
				if err != nil || !bytes.Equal(opened, payload) {
					errorsFound <- fmt.Errorf("worker %d Open = %x / %v", worker, opened, err)
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}

func mustProtectorDescription(ctx context.Context, t *testing.T, protector protect.Protector) protect.Description {
	t.Helper()
	description, err := protector.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	return description
}

func protectorBinding() protect.Binding {
	return protect.Binding{
		Component: "human-llm", Purpose: "payload-at-rest",
		Authority: "tenant-a", Namespace: "workspace-a", RecordType: "request",
		RecordID: "request-a", Field: "canonical-request", Schema: 1,
	}
}

func differentToken(current, candidate string) string {
	if current != candidate {
		return candidate
	}
	return candidate + "-other"
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
