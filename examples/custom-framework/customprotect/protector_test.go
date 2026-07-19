package customprotect_test

import (
	"context"
	"testing"
	"time"

	"github.com/vibe-agi/human/examples/custom-framework/customprotect"
	"github.com/vibe-agi/human/protect"
	protectaead "github.com/vibe-agi/human/protect/aead"
)

func TestOpenLocalOwnsKeyringAndPreservesNil(t *testing.T) {
	resource, err := customprotect.OpenLocal(t.Context(), localConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resource.Owned() {
		t.Fatal("OpenLocal returned a borrowed resource")
	}
	defer release(t, resource)
	protector, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}

	nilEnvelope, err := protector.Seal(t.Context(), testBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	nilPlaintext, err := protector.Open(t.Context(), testBinding(), nilEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	if nilPlaintext != nil {
		t.Fatalf("opened nil plaintext as non-nil %#v", nilPlaintext)
	}

	emptyEnvelope, err := protector.Seal(t.Context(), testBinding(), []byte{})
	if err != nil {
		t.Fatal(err)
	}
	emptyPlaintext, err := protector.Open(t.Context(), testBinding(), emptyEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	if emptyPlaintext == nil || len(emptyPlaintext) != 0 {
		t.Fatalf("opened empty plaintext as %#v", emptyPlaintext)
	}
}

func TestBorrowDoesNotReleaseHostProtector(t *testing.T) {
	baseResource, err := protectaead.Open(t.Context(), localConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer release(t, baseResource)
	base, err := baseResource.Value()
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := customprotect.Borrow(t.Context(), base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if borrowed.Owned() {
		t.Fatal("Borrow returned an owned resource")
	}
	if err := borrowed.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := base.Describe(t.Context()); err != nil {
		t.Fatalf("borrowed release closed the host Protector: %v", err)
	}
}

func TestInvalidBindingDoesNotEnterAuditMetadata(t *testing.T) {
	var observed customprotect.AuditEvent
	resource, err := customprotect.OpenLocal(t.Context(), localConfig(), func(event customprotect.AuditEvent) {
		observed = event
	})
	if err != nil {
		t.Fatal(err)
	}
	defer release(t, resource)
	protector, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protector.Seal(t.Context(), protect.Binding{Component: "secret\nvalue"}, []byte("payload")); err == nil {
		t.Fatal("Seal accepted an invalid binding")
	}
	if observed.Operation != "seal" || observed.Succeeded || observed.Component != "" ||
		observed.Purpose != "" || observed.RecordType != "" || observed.Field != "" {
		t.Fatalf("invalid binding leaked into audit metadata: %+v", observed)
	}
}

func localConfig() protectaead.Config {
	return protectaead.Config{
		Active: protectaead.KeyRef{ID: "test-key", Version: "1"},
		Keys: []protectaead.Key{{
			ID: "test-key", Version: "1", Material: make([]byte, protectaead.KeySize),
		}},
	}
}

func testBinding() protect.Binding {
	return protect.Binding{
		Component: "example", Purpose: "payload", Authority: "caller-a",
		Namespace: "deployment-a", RecordType: "request", RecordID: "request-a",
		Field: "body", Schema: 1,
	}
}

func release(t *testing.T, resource protect.Resource) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := resource.Release(ctx); err != nil {
		t.Error(err)
	}
}
