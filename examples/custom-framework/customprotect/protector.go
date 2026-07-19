// Package customprotect demonstrates an application-owned protect.Protector
// adapter. It adds policy/audit behavior around Human's reviewed local AEAD
// implementation instead of implementing cryptography itself.
package customprotect

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/protect"
	protectaead "github.com/vibe-agi/human/protect/aead"
)

// AuditEvent contains intentionally low-cardinality, non-secret metadata. It
// excludes authority, namespace, record ID, key identity, plaintext, and
// ciphertext so an audit sink cannot accidentally become another payload store.
type AuditEvent struct {
	Operation  string
	Component  string
	Purpose    string
	RecordType string
	Field      string
	Succeeded  bool
}

// AuditFunc must be concurrency-safe, bounded, and must not panic. Production
// adapters commonly enqueue this metadata into an application-owned audit
// pipeline rather than performing remote I/O inline.
type AuditFunc func(AuditEvent)

type protector struct {
	next        protect.Protector
	description protect.Description
	audit       AuditFunc
}

var _ protect.Protector = (*protector)(nil)

// Borrow decorates a host-owned Protector. Releasing the returned Resource is
// a no-op; the host must keep next alive until the Human runtime reaches Done.
func Borrow(
	ctx context.Context,
	next protect.Protector,
	audit AuditFunc,
) (protect.Resource, error) {
	decorated, err := wrap(ctx, next, audit)
	if err != nil {
		return protect.Resource{}, err
	}
	return framework.Borrow[protect.Protector](decorated), nil
}

// OpenLocal opens the official AES-256-GCM keyring, decorates it, and returns
// one owned Resource. Releasing the decorator releases and wipes the keyring's
// copied key material exactly once.
func OpenLocal(
	ctx context.Context,
	config protectaead.Config,
	audit AuditFunc,
) (protect.Resource, error) {
	baseResource, err := protectaead.Open(ctx, config)
	if err != nil {
		return protect.Resource{}, err
	}
	base, err := baseResource.Value()
	if err != nil {
		_ = release(baseResource)
		return protect.Resource{}, err
	}
	decorated, err := wrap(ctx, base, audit)
	if err != nil {
		return protect.Resource{}, errors.Join(err, release(baseResource))
	}
	resource, err := framework.Own[protect.Protector](decorated, baseResource.Release)
	if err != nil {
		return protect.Resource{}, errors.Join(err, release(baseResource))
	}
	return resource, nil
}

func wrap(ctx context.Context, next protect.Protector, audit AuditFunc) (*protector, error) {
	if ctx == nil {
		return nil, errors.New("custom protector: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nilInterface(next) {
		return nil, errors.New("custom protector: wrapped Protector is required")
	}
	description, err := next.Describe(ctx)
	if err != nil {
		return nil, err
	}
	if err := protect.ValidateDescription(description); err != nil {
		return nil, err
	}
	contract, err := framework.Negotiate(description.Contract, protect.Requirements())
	if err != nil {
		return nil, err
	}
	// Provider and Format are persisted envelope identities. A transparent
	// cryptographic decorator must preserve them rather than relabel ciphertext
	// that was authenticated by the underlying implementation.
	description.Contract = contract
	return &protector{next: next, description: description, audit: audit}, nil
}

func (decorator *protector) Describe(ctx context.Context) (protect.Description, error) {
	if ctx == nil {
		return protect.Description{}, errors.New("custom protector: context is required")
	}
	if err := ctx.Err(); err != nil {
		return protect.Description{}, err
	}
	description := decorator.description
	description.Contract.Features = maps.Clone(description.Contract.Features)
	return description, nil
}

func (decorator *protector) Seal(
	ctx context.Context,
	binding protect.Binding,
	plaintext []byte,
) (protect.Envelope, error) {
	// plaintext is borrowed for this call by the Protector contract; forwarding
	// it avoids creating another sensitive heap copy.
	envelope, err := decorator.next.Seal(ctx, binding, plaintext)
	decorator.record("seal", binding, err == nil)
	if err != nil {
		return protect.Envelope{}, err
	}
	return protect.CloneEnvelope(envelope), nil
}

func (decorator *protector) Open(
	ctx context.Context,
	binding protect.Binding,
	envelope protect.Envelope,
) ([]byte, error) {
	plaintext, err := decorator.next.Open(ctx, binding, protect.CloneEnvelope(envelope))
	decorator.record("open", binding, err == nil)
	if err != nil {
		return nil, err
	}
	owned := cloneBytes(plaintext)
	clear(plaintext)
	return owned, nil
}

func (decorator *protector) record(operation string, binding protect.Binding, succeeded bool) {
	if decorator.audit == nil {
		return
	}
	event := AuditEvent{Operation: operation, Succeeded: succeeded}
	// Never copy attacker-shaped metadata into an audit record. Human core emits
	// valid bindings, while a direct invalid call gets only operation/outcome.
	if protect.ValidateBinding(binding) == nil {
		event.Component, event.Purpose = binding.Component, binding.Purpose
		event.RecordType, event.Field = binding.RecordType, binding.Field
	}
	decorator.audit(event)
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

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func release(resource protect.Resource) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return resource.Release(ctx)
}
