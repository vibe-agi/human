package llm

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vibe-agi/human/framework"
)

type storeContractStub struct {
	bindings int
	views    int
	updates  int
}

func (store *storeContractStub) Bind(context.Context, StoreBinding) error {
	store.bindings++
	return nil
}

func (*storeContractStub) Description() StoreDescription {
	return StoreDescription{
		Contract: framework.Contract{ID: StoreContractID, Major: StoreContractMajor},
		Provider: "test-store", Version: "1",
	}
}

func (store *storeContractStub) View(_ context.Context, callback func(StoreView) error) error {
	store.views++
	return callback(nil)
}

func (store *storeContractStub) Update(_ context.Context, callback func(StoreTx) error) error {
	store.updates++
	return callback(nil)
}

var _ Store = (*storeContractStub)(nil)

func TestStoreDescriptionNegotiatesFrameworkContract(t *testing.T) {
	description := (&storeContractStub{}).Description()
	if err := description.Validate(); err != nil {
		t.Fatalf("valid Store description: %v", err)
	}
	description.Contract.Major++
	if err := description.Validate(); !errors.Is(err, framework.ErrContractMismatch) ||
		!errors.Is(err, ErrStoreContractMismatch) {
		t.Fatalf("major mismatch error = %v", err)
	}
}

func TestStoreDescriptionRejectsMalformedMetadata(t *testing.T) {
	description := (&storeContractStub{}).Description()
	for _, provider := range []string{"", " sqlite", "sqlite\npassword=secret"} {
		description.Provider = provider
		if err := description.Validate(); !errors.Is(err, ErrStoreDescription) {
			t.Fatalf("provider %q error = %v", provider, err)
		}
	}
}

func TestStoreCallbackShapeIsExactlyOnceAndPropagatesError(t *testing.T) {
	store := &storeContractStub{}
	want := errors.New("stop")
	if err := store.View(context.Background(), func(StoreView) error { return want }); !errors.Is(err, want) || store.views != 1 {
		t.Fatalf("View error/calls = %v/%d", err, store.views)
	}
	if err := store.Update(context.Background(), func(StoreTx) error { return want }); !errors.Is(err, want) || store.updates != 1 {
		t.Fatalf("Update error/calls = %v/%d", err, store.updates)
	}
}

func TestStoreBusinessPortHasNoCloseMethod(t *testing.T) {
	storeType := reflect.TypeOf((*Store)(nil)).Elem()
	if _, exists := storeType.MethodByName("Close"); exists {
		t.Fatal("Store lifecycle leaked into the business port")
	}
}

func TestStoreTypedErrorsPreserveSentinelsAndCauses(t *testing.T) {
	if err := (&StoreNotFoundError{Record: StoreRecordRequest, Key: "caller/request"}); !errors.Is(err, ErrStoreRecordNotFound) {
		t.Fatalf("not-found error lost sentinel: %v", err)
	}
	if err := (&StoreConflictError{Constraint: StoreConstraintRequestKey, Key: "caller/request"}); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("conflict error lost sentinel: %v", err)
	}
	if err := (&StoreLimitError{Record: StoreRecordResponseEvent, Limit: 1024}); !errors.Is(err, ErrStoreRecordTooLarge) {
		t.Fatalf("limit error lost sentinel: %v", err)
	}
	cause := errors.New("connection dropped during commit")
	unknown := &StoreCommitUnknownError{Cause: cause}
	if !errors.Is(unknown, ErrStoreCommitUnknown) || !errors.Is(unknown, cause) {
		t.Fatalf("commit-unknown error lost classification: %v", unknown)
	}
	corrupt := &StoreCorruptError{Record: StoreRecordTask, Key: "caller/task", Cause: cause}
	if !errors.Is(corrupt, ErrStoreCorruptRecord) || !errors.Is(corrupt, cause) {
		t.Fatalf("corrupt-record error lost classification: %v", corrupt)
	}
}

func TestCodecSnapshotPinsFullNegotiatedIdentity(t *testing.T) {
	feature := framework.Feature("deterministic-seed")
	description := CodecDescription{
		Contract: framework.Contract{
			ID: CodecContractID, Major: RequiredCodecContract().Major,
			Features: map[framework.Feature]uint16{feature: 1},
		},
		ID: "example.chat", Version: "2026.07.19",
		Fingerprint: Fingerprint([]byte("implementation+configuration")),
		Limits: CodecLimits{
			MaxRequestBytes: 1024, MaxStreamFrameBytes: 1024,
			MaxStreamFramesPerStep: 4, MaxAggregateBytes: 2048,
			MaxAdmissionErrorBytes: 512,
		},
		OverloadedStatus: 503,
	}
	snapshot, err := NewCodecSnapshot(description)
	if err != nil {
		t.Fatalf("snapshot codec: %v", err)
	}
	if snapshot.ID != description.ID || snapshot.Version != description.Version ||
		snapshot.Fingerprint != description.Fingerprint ||
		snapshot.Contract.ID != CodecContractID || snapshot.Contract.Features[feature] != 1 {
		t.Fatalf("snapshot lost codec identity: %#v", snapshot)
	}
	// NegotiateCodec must freeze the contract map instead of retaining a caller
	// alias that can change recovery identity after construction.
	description.Contract.Features[feature] = 9
	if snapshot.Contract.Features[feature] != 1 {
		t.Fatalf("snapshot retained mutable contract feature map: %#v", snapshot.Contract.Features)
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("validate persisted snapshot: %v", err)
	}
	equal := snapshot
	equal.Contract.Features = map[framework.Feature]uint16{feature: 1}
	if !snapshot.Equal(equal) {
		t.Fatal("equivalent contract feature maps changed codec identity")
	}
	equal.Contract.Minor++
	if snapshot.Equal(equal) {
		t.Fatal("codec contract snapshot mismatch was ignored")
	}
	snapshot.Fingerprint = "sha256:not-a-digest"
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidCodecContract) {
		t.Fatalf("invalid persisted fingerprint error = %v", err)
	}
}

func TestTaskStateVocabularySeparatesTerminalStates(t *testing.T) {
	for _, state := range []TaskState{
		TaskLeased, TaskAwaitingHuman, TaskAwaitingCaller, TaskAwaitingResults,
	} {
		if !state.Valid() || state.Terminal() {
			t.Fatalf("non-terminal state classification = %q", state)
		}
	}
	for _, state := range []TaskState{
		TaskCompleted, TaskRejected, TaskExpired, TaskFailed,
	} {
		if !state.Valid() || !state.Terminal() {
			t.Fatalf("terminal state classification = %q", state)
		}
	}
	if TaskState("future-state").Valid() || TaskState("future-state").Terminal() {
		t.Fatal("unknown state was accepted")
	}
	for _, dead := range []TaskState{"admitted", "responded", "tools_dispatched", "reconciled", "canceled"} {
		if dead.Valid() || dead.Terminal() {
			t.Fatalf("unreachable state remained public: %q", dead)
		}
	}
}

func TestRequestRecordCarriesCrashStableDispatchAndEncoderIdentity(t *testing.T) {
	record := StoreRequestRecord{
		Key:        StoreRequestKey{Caller: "caller-a", IdempotencyKey: "request-key-a"},
		Task:       StoreTaskKey{Caller: "caller-a", Task: "task-a"},
		RequestID:  "request-a",
		ResponseID: "response-a",
	}
	head := StoreRequestHead{
		Key: record.Key, Task: record.Task,
		RequestID: record.RequestID, ResponseID: record.ResponseID,
	}
	if head.RequestID != "request-a" || head.ResponseID != "response-a" {
		t.Fatalf("request head lost recovery identity: %#v", head)
	}
	if head.RequestID == head.ResponseID {
		t.Fatal("request delivery identity was conflated with codec response identity")
	}
}
