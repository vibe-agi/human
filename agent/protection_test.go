package agent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/protect"
	protectaead "github.com/vibe-agi/human/protect/aead"
	"github.com/vibe-agi/human/workspace"
)

func TestProtectedAgentStoresSealedMessagesAndArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "protected-agent.db")
	service := openProtectedAgent(t, path, keyringConfig("v1", testKey('a')))
	ctx := t.Context()
	contextRef, taskRef := refs("protected-tenant", "context", "workspace", "task")
	messageSecret := "message-plaintext-canary"
	working := createWorkingTaskWithText(t, service, contextRef, taskRef, "protected", messageSecret)
	artifactSecret := []byte("artifact-plaintext-canary")
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(
		t, service, "protected", working, "workspace-base", artifactSecret,
	))
	if err != nil {
		t.Fatal(err)
	}

	var messageWire, artifactWire []byte
	if err := service.database.QueryRowContext(ctx, `
		SELECT parts FROM agent_messages
		WHERE authority_id = ? AND message_id = ?`, taskRef.Workspace.Authority, "initial-protected",
	).Scan(&messageWire); err != nil {
		t.Fatal(err)
	}
	if err := service.database.QueryRowContext(ctx, `
		SELECT payload FROM agent_artifacts
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		taskRef.Workspace.Authority, taskRef.Workspace.ID, frozen.Artifact.Ref.ID,
	).Scan(&artifactWire); err != nil {
		t.Fatal(err)
	}
	assertSealedAtRest(t, messageWire, []byte(messageSecret))
	assertSealedAtRest(t, artifactWire, artifactSecret)

	messages, err := service.ListMessages(ctx, taskRef, PageRequest{})
	if err != nil || len(messages.Items) != 1 || string(messages.Items[0].Parts[0].Data) != messageSecret {
		t.Fatalf("read protected message = %#v / %v", messages, err)
	}
	artifact, err := service.GetArtifact(ctx, frozen.Artifact.Ref)
	if err != nil || !bytes.Equal(artifact.Payload.Data, artifactSecret) {
		t.Fatalf("read protected Artifact = %#v / %v", artifact, err)
	}
}

func TestProtectedAgentRejectsRecordSwapAndPlaintextDowngrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "protected-agent.db")
	config := keyringConfig("v1", testKey('s'))
	service := openProtectedAgent(t, path, config)
	ctx := t.Context()
	contextRef, firstRef := refs("swap-tenant", "context", "workspace", "task-a")
	_, secondRef := refs("swap-tenant", "context", "workspace", "task-b")
	if _, err := service.CreateTask(ctx, createCommand("create-a", contextRef, firstRef, "message-a", "alpha")); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTask(ctx, createCommand("create-b", contextRef, secondRef, "message-b", "bravo")); err != nil {
		t.Fatal(err)
	}
	var firstWire, secondWire []byte
	if err := service.database.QueryRowContext(ctx, `SELECT parts FROM agent_messages WHERE authority_id = ? AND message_id = ?`,
		firstRef.Workspace.Authority, "message-a").Scan(&firstWire); err != nil {
		t.Fatal(err)
	}
	if err := service.database.QueryRowContext(ctx, `SELECT parts FROM agent_messages WHERE authority_id = ? AND message_id = ?`,
		firstRef.Workspace.Authority, "message-b").Scan(&secondWire); err != nil {
		t.Fatal(err)
	}
	if _, err := service.database.ExecContext(ctx, `UPDATE agent_messages SET parts = CASE message_id
		WHEN 'message-a' THEN ? WHEN 'message-b' THEN ? END
		WHERE authority_id = ? AND message_id IN ('message-a','message-b')`,
		secondWire, firstWire, firstRef.Workspace.Authority); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetMessage(ctx, firstRef.Workspace.Authority, "message-a"); !errors.Is(err, ErrCorruptStore) || !errors.Is(err, protect.ErrAuthentication) {
		t.Fatalf("swapped message error = %v, want corrupt/authentication", err)
	}

	// Restore A, then replace its authenticated frame with a well-formed plain
	// frame carrying the exact canonical content and digest. Default policy must
	// still reject the mode downgrade.
	parts := textMessage("message-a", "alpha").Parts
	canonical, err := json.Marshal(parts)
	if err != nil {
		t.Fatal(err)
	}
	plainWire, err := protect.MarshalStoredValue(protect.NewPlainStoredValue(canonical))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.database.ExecContext(ctx, `UPDATE agent_messages SET parts = ?
		WHERE authority_id = ? AND message_id = ?`, plainWire, firstRef.Workspace.Authority, "message-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetMessage(ctx, firstRef.Workspace.Authority, "message-a"); !errors.Is(err, ErrProtectionDowngrade) || !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("plaintext downgrade error = %v", err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	migration := openProtectedAgentWithPolicy(t, path, config, true)
	message, err := migration.GetMessage(ctx, firstRef.Workspace.Authority, "message-a")
	if err != nil || string(message.Parts[0].Data) != "alpha" {
		t.Fatalf("explicit migration read = %#v / %v", message, err)
	}
}

func TestProtectedArtifactAuthenticatesPayloadSemanticBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact-aad.db")
	service := openProtectedAgent(t, path, keyringConfig("v1", testKey('m')))
	ctx := t.Context()
	contextRef, taskRef := refs("metadata-tenant", "context", "workspace", "task")
	working := createWorkingTaskWithText(t, service, contextRef, taskRef, "metadata", "edit workspace")
	frozen, err := service.FreezeArtifact(ctx, freezeCommand(
		t, service, "metadata", working, "workspace-base", []byte("authenticated payload"),
	))
	if err != nil {
		t.Fatal(err)
	}
	tamperedMediaType := "text/plain"
	tamperedDigest := digestArtifact(
		frozen.Artifact.Ref, frozen.Artifact.Task,
		frozen.Artifact.BaseRevision, frozen.Artifact.ResultRevision,
		frozen.Artifact.PayloadDigest, tamperedMediaType,
	)
	if _, err := service.database.ExecContext(ctx, `
		UPDATE agent_artifacts SET media_type = ?, artifact_digest = ?
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		tamperedMediaType, tamperedDigest,
		frozen.Artifact.Ref.Workspace.Authority, frozen.Artifact.Ref.Workspace.ID, frozen.Artifact.Ref.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetArtifact(ctx, frozen.Artifact.Ref); !errors.Is(err, ErrCorruptStore) || !errors.Is(err, protect.ErrAuthentication) {
		t.Fatalf("tampered Artifact metadata error = %v, want corrupt/authentication", err)
	}
}

func TestProtectedAgentReopensAcrossKeyRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rotated-agent.db")
	oldKey, newKey := testKey('o'), testKey('n')
	oldConfig := keyringConfig("v1", oldKey)
	old := openProtectedAgent(t, path, oldConfig)
	ctx := t.Context()
	contextRef, oldRef := refs("rotate-tenant", "context", "workspace", "old-task")
	if _, err := old.CreateTask(ctx, createCommand("create-old", contextRef, oldRef, "message-old", "old payload")); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	rotatedConfig := protectaead.Config{
		Active: protectaead.KeyRef{ID: "payload-key", Version: "v2"},
		Keys: []protectaead.Key{
			{ID: "payload-key", Version: "v1", Material: oldKey},
			{ID: "payload-key", Version: "v2", Material: newKey},
		},
	}
	rotated := openProtectedAgent(t, path, rotatedConfig)
	if message, err := rotated.GetMessage(ctx, oldRef.Workspace.Authority, "message-old"); err != nil || string(message.Parts[0].Data) != "old payload" {
		t.Fatalf("rotated read old message = %#v / %v", message, err)
	}
	_, newRef := refs("rotate-tenant", "context", "workspace", "new-task")
	if _, err := rotated.CreateTask(ctx, createCommand("create-new", contextRef, newRef, "message-new", "new payload")); err != nil {
		t.Fatal(err)
	}
	assertStoredMessageKeyVersion(t, rotated, "message-old", "v1")
	assertStoredMessageKeyVersion(t, rotated, "message-new", "v2")
	if err := rotated.Close(); err != nil {
		t.Fatal(err)
	}

	current := openProtectedAgent(t, path, keyringConfig("v2", newKey))
	if message, err := current.GetMessage(ctx, newRef.Workspace.Authority, "message-new"); err != nil || string(message.Parts[0].Data) != "new payload" {
		t.Fatalf("current-only read new message = %#v / %v", message, err)
	}
	if _, err := current.GetMessage(ctx, oldRef.Workspace.Authority, "message-old"); !errors.Is(err, protect.ErrKeyUnavailable) {
		t.Fatalf("current-only old message error = %v, want ErrKeyUnavailable", err)
	}
}

func TestCommitUnknownRetryDoesNotResealCommittedMessage(t *testing.T) {
	base, _ := openTestAgent(t)
	store := &commitUnknownOnceAgentStore{Store: base.store}
	protectorResource, err := protectaead.Open(t.Context(), keyringConfig("v1", testKey('c')))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = protectorResource.Release(context.Background()) })
	delegate, _ := protectorResource.Value()
	counted := &countingProtector{Protector: delegate}
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](store)
	config.Protector = framework.Borrow[protect.Protector](counted)
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	contextRef, taskRef := refs("retry-tenant", "context", "workspace", "task")
	command := createCommand("create", contextRef, taskRef, "message", "retry payload")
	if _, err := service.CreateTask(t.Context(), command); !errors.Is(err, ErrStoreCommitUnknown) {
		t.Fatalf("first create error = %v, want commit unknown", err)
	}
	if got := counted.seals.Load(); got != 1 {
		t.Fatalf("Seal count after ambiguous commit = %d, want 1", got)
	}
	if _, err := service.CreateTask(t.Context(), command); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if got := counted.seals.Load(); got != 1 {
		t.Fatalf("exact committed retry resealed content: %d", got)
	}
}

func TestFreezeArtifactExactReplayDoesNotRequireProtectorOpen(t *testing.T) {
	base, _ := openTestAgent(t)
	contextRef, taskRef := refs("artifact-replay-tenant", "context", "workspace", "task")
	working := createWorkingTaskWithText(t, base, contextRef, taskRef, "artifact-replay", "edit workspace")

	store := &commitUnknownOnceAgentStore{Store: base.store}
	protectorResource, err := protectaead.Open(t.Context(), keyringConfig("v1", testKey('r')))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = protectorResource.Release(context.Background()) })
	delegate, err := protectorResource.Value()
	if err != nil {
		t.Fatal(err)
	}
	protector := &unavailableOpenProtector{Protector: delegate}
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](store)
	config.Protector = framework.Borrow[protect.Protector](protector)
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })

	command := freezeCommand(
		t, service, "artifact-replay", working, "workspace-base", []byte("durable artifact"),
	)
	if _, err := service.FreezeArtifact(t.Context(), command); !errors.Is(err, ErrStoreCommitUnknown) {
		t.Fatalf("first FreezeArtifact error = %v, want commit unknown", err)
	}
	protector.unavailable.Store(true)
	if _, err := service.FreezeArtifact(t.Context(), command); err != nil {
		t.Fatalf("exact FreezeArtifact retry required KMS: %v", err)
	}
	if got := protector.opens.Load(); got != 0 {
		t.Fatalf("exact FreezeArtifact retry Open calls = %d, want 0", got)
	}
}

func TestAgentEnforcesFrozenProtectorDescription(t *testing.T) {
	baseDescription := protect.Description{
		Contract: framework.Contract{ID: protect.ContractID, Major: protect.ContractMajor},
		Provider: "test-kms", Format: "sealed-v1",
		MaxPlaintextBytes: 4096, MaxEnvelopeBytes: 8192,
	}
	validEnvelope := func() protect.Envelope {
		return protect.Envelope{
			Provider: "test-kms", Format: "sealed-v1", KeyID: "key", KeyVersion: "1",
			Data: []byte{1},
		}
	}

	t.Run("reject plaintext before Seal", func(t *testing.T) {
		description := baseDescription
		description.MaxPlaintextBytes = 1
		provider := &contractTestProtector{description: description}
		service := newAgentWithTestProtector(t, provider)
		contextRef, taskRef := refs("contract-tenant", "context", "workspace", "oversized")
		_, err := service.CreateTask(t.Context(), createCommand(
			"create-oversized", contextRef, taskRef, "message-oversized", "too large",
		))
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("seal oversized plaintext error = %v, want ErrInvalidArgument", err)
		}
		if got := provider.seals.Load(); got != 0 {
			t.Fatalf("Seal calls = %d, want 0", got)
		}
	})

	t.Run("reject mismatched provider after Seal", func(t *testing.T) {
		provider := &contractTestProtector{description: baseDescription}
		provider.seal = func([]byte) protect.Envelope {
			envelope := validEnvelope()
			envelope.Provider = "other-kms"
			return envelope
		}
		service := newAgentWithTestProtector(t, provider)
		contextRef, taskRef := refs("contract-tenant", "context", "workspace", "provider")
		_, err := service.CreateTask(t.Context(), createCommand(
			"create-provider", contextRef, taskRef, "message-provider", "payload",
		))
		if !errors.Is(err, protect.ErrInvalidEnvelope) {
			t.Fatalf("seal mismatched provider error = %v, want invalid envelope", err)
		}
	})

	t.Run("reject oversized envelope after Seal", func(t *testing.T) {
		description := baseDescription
		description.MaxPlaintextBytes = 128
		description.MaxEnvelopeBytes = 128
		provider := &contractTestProtector{description: description}
		provider.seal = func([]byte) protect.Envelope {
			envelope := validEnvelope()
			envelope.Nonce = []byte{1}
			envelope.Data = make([]byte, 128)
			return envelope
		}
		service := newAgentWithTestProtector(t, provider)
		contextRef, taskRef := refs("contract-tenant", "context", "workspace", "envelope")
		_, err := service.CreateTask(t.Context(), createCommand(
			"create-envelope", contextRef, taskRef, "message-envelope", "payload",
		))
		if !errors.Is(err, protect.ErrInvalidEnvelope) {
			t.Fatalf("seal oversized envelope error = %v, want invalid envelope", err)
		}
	})

	t.Run("reject mismatched provider before Open", func(t *testing.T) {
		var plaintext []byte
		provider := &contractTestProtector{description: baseDescription}
		provider.seal = func(value []byte) protect.Envelope {
			plaintext = append([]byte(nil), value...)
			return validEnvelope()
		}
		provider.open = func(protect.Envelope) []byte { return append([]byte(nil), plaintext...) }
		service := newAgentWithTestProtector(t, provider)
		contextRef, taskRef := refs("contract-tenant", "context", "workspace", "open-provider")
		if _, err := service.CreateTask(t.Context(), createCommand(
			"create-open-provider", contextRef, taskRef, "message-open-provider", "payload",
		)); err != nil {
			t.Fatal(err)
		}
		var encoded []byte
		if err := service.database.QueryRowContext(t.Context(), `
			SELECT parts FROM agent_messages WHERE authority_id = ? AND message_id = ?`,
			taskRef.Workspace.Authority, "message-open-provider",
		).Scan(&encoded); err != nil {
			t.Fatal(err)
		}
		value, err := protect.UnmarshalStoredValue(encoded)
		if err != nil {
			t.Fatal(err)
		}
		value.Envelope.Format = "other-v1"
		encoded, err = protect.MarshalStoredValue(value)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.database.ExecContext(t.Context(), `
			UPDATE agent_messages SET parts = ? WHERE authority_id = ? AND message_id = ?`,
			encoded, taskRef.Workspace.Authority, "message-open-provider",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := service.GetMessage(t.Context(), taskRef.Workspace.Authority, "message-open-provider"); !errors.Is(err, ErrCorruptStore) || !errors.Is(err, protect.ErrInvalidEnvelope) {
			t.Fatalf("open mismatched format error = %v, want corrupt/invalid envelope", err)
		}
		if got := provider.opens.Load(); got != 0 {
			t.Fatalf("Open calls = %d, want 0", got)
		}
	})

	t.Run("reject oversized plaintext after Open", func(t *testing.T) {
		provider := &contractTestProtector{description: baseDescription}
		provider.seal = func([]byte) protect.Envelope { return validEnvelope() }
		provider.open = func(protect.Envelope) []byte {
			return bytes.Repeat([]byte{'x'}, int(baseDescription.MaxPlaintextBytes)+1)
		}
		service := newAgentWithTestProtector(t, provider)
		contextRef, taskRef := refs("contract-tenant", "context", "workspace", "open-size")
		if _, err := service.CreateTask(t.Context(), createCommand(
			"create-open-size", contextRef, taskRef, "message-open-size", "payload",
		)); err != nil {
			t.Fatal(err)
		}
		if _, err := service.GetMessage(t.Context(), taskRef.Workspace.Authority, "message-open-size"); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("open oversized plaintext error = %v, want ErrCorruptStore", err)
		}
	})
}

func TestProtectorIOOccursOutsideStoreCallbacks(t *testing.T) {
	base, _ := openTestAgent(t)
	probe := &callbackProbeStore{Store: base.store}
	protectorResource, err := protectaead.Open(t.Context(), keyringConfig("v1", testKey('p')))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = protectorResource.Release(context.Background()) })
	delegate, _ := protectorResource.Value()
	blocked := &blockingProtector{
		Protector: delegate, activeCallbacks: &probe.active,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](probe)
	config.Protector = framework.Borrow[protect.Protector](blocked)
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	contextRef, taskRef := refs("probe-tenant", "context", "workspace", "task")
	done := make(chan error, 1)
	go func() {
		_, err := service.CreateTask(context.Background(), createCommand(
			"create", contextRef, taskRef, "message", "blocked KMS",
		))
		done <- err
	}()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Protector Seal was not reached")
	}
	if blocked.insideCallback.Load() {
		t.Fatal("Protector Seal ran inside a Store callback")
	}
	viewCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := probe.View(viewCtx, func(StoreView) error { return nil }); err != nil {
		t.Fatalf("Store callback blocked behind KMS I/O: %v", err)
	}
	close(blocked.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if blocked.insideCallback.Load() {
		t.Fatal("Protector operation overlapped its calling Store callback")
	}
	message, err := service.GetMessage(t.Context(), taskRef.Workspace.Authority, "message")
	if err != nil || string(message.Parts[0].Data) != "blocked KMS" {
		t.Fatalf("read protected message = %#v / %v", message, err)
	}
	if blocked.insideCallback.Load() {
		t.Fatal("Protector Open ran inside a Store callback")
	}
	if got := blocked.opens.Load(); got != 1 {
		t.Fatalf("Protector Open calls = %d, want 1", got)
	}
}

func TestProtectorAndStoreReleaseInReverseOrder(t *testing.T) {
	base, _ := openTestAgent(t)
	protectorResource, err := protectaead.Open(t.Context(), keyringConfig("v1", testKey('l')))
	if err != nil {
		t.Fatal(err)
	}
	delegate, _ := protectorResource.Value()
	var mu sync.Mutex
	var order []string
	storeResource, _ := framework.Own[Store](base.store, func(context.Context) error {
		mu.Lock()
		order = append(order, "store")
		mu.Unlock()
		return nil
	})
	ownedProtector, _ := framework.Own[protect.Protector](delegate, func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "protector")
		mu.Unlock()
		return protectorResource.Release(ctx)
	})
	config := DefaultConfig()
	config.Store, config.Protector = storeResource, ownedProtector
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "protector" || order[1] != "store" {
		t.Fatalf("release order = %v, want [protector store]", order)
	}
}

func TestInvalidProtectorReleasesOwnedDependenciesInReverseOrder(t *testing.T) {
	base, _ := openTestAgent(t)
	var mu sync.Mutex
	var order []string
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}
	storeResource, err := framework.Own[Store](base.store, func(context.Context) error {
		record("store")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	protectorResource, err := framework.Own[protect.Protector](invalidDescriptionProtector{}, func(context.Context) error {
		record("protector")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Store, config.Protector = storeResource, protectorResource
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("New accepted an invalid Protector description")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "protector" || order[1] != "store" {
		t.Fatalf("constructor cleanup order = %v, want [protector store]", order)
	}
}

func TestAgentDoesNotReleaseBorrowedProtector(t *testing.T) {
	base, _ := openTestAgent(t)
	resource, err := protectaead.Open(t.Context(), keyringConfig("v1", testKey('b')))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resource.Release(context.Background()) })
	protector, err := resource.Value()
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](base.store)
	config.Protector = framework.Borrow[protect.Protector](protector)
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := protector.Describe(t.Context()); err != nil {
		t.Fatalf("borrowed Protector was released by Agent: %v", err)
	}
}

type countingProtector struct {
	protect.Protector
	seals atomic.Int32
}

type unavailableOpenProtector struct {
	protect.Protector
	unavailable atomic.Bool
	opens       atomic.Int32
}

func (protector *unavailableOpenProtector) Open(
	ctx context.Context,
	binding protect.Binding,
	envelope protect.Envelope,
) ([]byte, error) {
	protector.opens.Add(1)
	if protector.unavailable.Load() {
		return nil, protect.ErrProviderUnavailable
	}
	return protector.Protector.Open(ctx, binding, envelope)
}

type contractTestProtector struct {
	description protect.Description
	seal        func([]byte) protect.Envelope
	open        func(protect.Envelope) []byte
	seals       atomic.Int32
	opens       atomic.Int32
}

func (protector *contractTestProtector) Describe(context.Context) (protect.Description, error) {
	return protector.description, nil
}

func (protector *contractTestProtector) Seal(
	_ context.Context,
	_ protect.Binding,
	plaintext []byte,
) (protect.Envelope, error) {
	protector.seals.Add(1)
	if protector.seal == nil {
		panic("unexpected contract test Seal")
	}
	return protector.seal(append([]byte(nil), plaintext...)), nil
}

func (protector *contractTestProtector) Open(
	_ context.Context,
	_ protect.Binding,
	envelope protect.Envelope,
) ([]byte, error) {
	protector.opens.Add(1)
	if protector.open == nil {
		panic("unexpected contract test Open")
	}
	return protector.open(protect.CloneEnvelope(envelope)), nil
}

func newAgentWithTestProtector(t *testing.T, protector protect.Protector) *testAgent {
	t.Helper()
	base, _ := openTestAgent(t)
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](base.store)
	config.Protector = framework.Borrow[protect.Protector](protector)
	service, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return &testAgent{Agent: service, store: base.store, database: base.database, now: base.now}
}

func TestAgentRejectsBorrowedTypedNilProtector(t *testing.T) {
	base, _ := openTestAgent(t)
	var typedNil *contractTestProtector
	config := DefaultConfig()
	config.Store = framework.Borrow[Store](base.store)
	config.Protector = framework.Borrow[protect.Protector](typedNil)
	service, err := New(t.Context(), config)
	if service != nil {
		_ = service.Close()
		t.Fatal("New returned a service for a borrowed typed-nil Protector")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("New error = %v, want ErrInvalidArgument", err)
	}
}

func (protector *countingProtector) Seal(ctx context.Context, binding protect.Binding, plaintext []byte) (protect.Envelope, error) {
	protector.seals.Add(1)
	return protector.Protector.Seal(ctx, binding, plaintext)
}

type commitUnknownOnceAgentStore struct {
	Store
	returned atomic.Bool
}

func (store *commitUnknownOnceAgentStore) Update(ctx context.Context, callback func(StoreTx) error) error {
	err := store.Store.Update(ctx, callback)
	if err == nil && store.returned.CompareAndSwap(false, true) {
		return &StoreCommitUnknownError{Cause: errors.New("injected ambiguous commit")}
	}
	return err
}

type callbackProbeStore struct {
	Store
	active atomic.Int32
}

func (store *callbackProbeStore) View(ctx context.Context, callback func(StoreView) error) error {
	return store.Store.View(ctx, func(view StoreView) error {
		store.active.Add(1)
		defer store.active.Add(-1)
		return callback(view)
	})
}

func (store *callbackProbeStore) Update(ctx context.Context, callback func(StoreTx) error) error {
	return store.Store.Update(ctx, func(tx StoreTx) error {
		store.active.Add(1)
		defer store.active.Add(-1)
		return callback(tx)
	})
}

type blockingProtector struct {
	protect.Protector
	activeCallbacks *atomic.Int32
	entered         chan struct{}
	release         chan struct{}
	insideCallback  atomic.Bool
	opens           atomic.Int32
	once            sync.Once
}

func (protector *blockingProtector) Seal(ctx context.Context, binding protect.Binding, plaintext []byte) (protect.Envelope, error) {
	if protector.activeCallbacks.Load() != 0 {
		protector.insideCallback.Store(true)
	}
	protector.once.Do(func() { close(protector.entered) })
	select {
	case <-protector.release:
	case <-ctx.Done():
		return protect.Envelope{}, ctx.Err()
	}
	return protector.Protector.Seal(ctx, binding, plaintext)
}

func (protector *blockingProtector) Open(ctx context.Context, binding protect.Binding, envelope protect.Envelope) ([]byte, error) {
	if protector.activeCallbacks.Load() != 0 {
		protector.insideCallback.Store(true)
	}
	protector.opens.Add(1)
	return protector.Protector.Open(ctx, binding, envelope)
}

type invalidDescriptionProtector struct{}

func (invalidDescriptionProtector) Describe(context.Context) (protect.Description, error) {
	return protect.Description{}, nil
}

func (invalidDescriptionProtector) Seal(context.Context, protect.Binding, []byte) (protect.Envelope, error) {
	panic("Seal must not run after an invalid description")
}

func (invalidDescriptionProtector) Open(context.Context, protect.Binding, protect.Envelope) ([]byte, error) {
	panic("Open must not run after an invalid description")
}

func openProtectedAgent(t *testing.T, path string, protectorConfig protectaead.Config) *testAgent {
	t.Helper()
	return openProtectedAgentWithPolicy(t, path, protectorConfig, false)
}

func openProtectedAgentWithPolicy(
	t *testing.T,
	path string,
	protectorConfig protectaead.Config,
	allowPlain bool,
) *testAgent {
	t.Helper()
	resource, err := protectaead.Open(t.Context(), protectorConfig)
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Protector = resource
	config.AllowPlaintextReads = allowPlain
	return openTestAgentWithConfig(t, path, config)
}

func keyringConfig(version string, material []byte) protectaead.Config {
	return protectaead.Config{
		Active: protectaead.KeyRef{ID: "payload-key", Version: version},
		Keys:   []protectaead.Key{{ID: "payload-key", Version: version, Material: material}},
	}
}

func testKey(value byte) []byte { return bytes.Repeat([]byte{value}, protectaead.KeySize) }

func createWorkingTaskWithText(
	t *testing.T,
	service interface {
		CreateTask(context.Context, CreateTaskCommand) (Task, error)
		AcquireLease(context.Context, AcquireLeaseCommand) (LeaseAssignment, error)
		AcceptTask(context.Context, WorkerTaskCommand) (Task, error)
	},
	contextRef ContextRef,
	taskRef TaskRef,
	suffix, text string,
) Task {
	t.Helper()
	created, err := service.CreateTask(t.Context(), createCommand(
		"create-"+suffix, contextRef, taskRef, "initial-"+suffix, text,
	))
	if err != nil {
		t.Fatal(err)
	}
	grant := acquireTestLease(t, service, taskRef)
	working, err := service.AcceptTask(t.Context(), WorkerTaskCommand{
		Meta: WorkerCommandMeta{ID: CommandID("accept-" + suffix), ExpectedRevision: created.Revision, Grant: grant},
		Task: taskRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	return working
}

func digestArtifact(
	ref ArtifactRef,
	task TaskRef,
	base, result workspace.Revision,
	payload workspace.Digest,
	mediaType string,
) workspace.Digest {
	encoded, _ := json.Marshal(struct {
		Ref       ArtifactRef        `json:"ref"`
		Task      TaskRef            `json:"task"`
		Base      workspace.Revision `json:"base"`
		Result    workspace.Revision `json:"result"`
		Payload   workspace.Digest   `json:"payload"`
		MediaType string             `json:"media_type"`
	}{ref, task, base, result, payload, mediaType})
	sum := sha256.Sum256(encoded)
	return workspace.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

func assertSealedAtRest(t *testing.T, encoded, plaintext []byte) {
	t.Helper()
	value, err := protect.UnmarshalStoredValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if value.Mode != protect.StoredValueSealed || value.Envelope == nil || value.Plain != nil {
		t.Fatalf("stored value is not sealed: %#v", value)
	}
	for _, representation := range [][]byte{
		plaintext,
		[]byte(base64.StdEncoding.EncodeToString(plaintext)),
	} {
		if len(representation) > 0 && bytes.Contains(encoded, representation) {
			t.Fatalf("stored frame contains plaintext representation %q", representation)
		}
	}
}

func assertStoredMessageKeyVersion(t *testing.T, service *testAgent, message MessageID, version string) {
	t.Helper()
	var encoded []byte
	if err := service.database.QueryRowContext(t.Context(), `SELECT parts FROM agent_messages WHERE message_id = ?`, message).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	value, err := protect.UnmarshalStoredValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if value.Envelope == nil || value.Envelope.KeyVersion != version {
		t.Fatalf("message %q key version = %#v, want %q", message, value.Envelope, version)
	}
}

var _ Store = (*commitUnknownOnceAgentStore)(nil)
var _ Store = (*callbackProbeStore)(nil)
var _ protect.Protector = (*countingProtector)(nil)
var _ protect.Protector = (*blockingProtector)(nil)
var _ protect.Protector = invalidDescriptionProtector{}
