package agent_test

import (
	"context"
	"errors"
	. "github.com/vibe-agi/human/agent"
	"testing"

	"github.com/vibe-agi/human/framework"
)

type contractStoreStub struct{}

func (contractStoreStub) Description() StoreDescription {
	return StoreDescription{
		Contract: framework.Contract{ID: StoreContractID, Major: StoreContractMajor},
		Provider: "test-store", Version: "1",
	}
}

func (contractStoreStub) View(_ context.Context, callback func(StoreView) error) error {
	return callback(nil)
}

func (contractStoreStub) Update(_ context.Context, callback func(StoreTx) error) error {
	return callback(nil)
}

var _ Store = contractStoreStub{}

func TestStoreDescriptionNegotiatesFrameworkContract(t *testing.T) {
	description := contractStoreStub{}.Description()
	if err := description.Validate(); err != nil {
		t.Fatalf("valid Store description: %v", err)
	}
	description.Contract.Major++
	if err := description.Validate(); !errors.Is(err, framework.ErrContractMismatch) ||
		!errors.Is(err, ErrStoreContractMismatch) {
		t.Fatalf("major mismatch error = %v", err)
	}
}

func TestStoreDescriptionRejectsSecretShapedOrControlMetadata(t *testing.T) {
	description := contractStoreStub{}.Description()
	for _, provider := range []string{"", " sqlite", "sqlite\npassword=secret"} {
		description.Provider = provider
		if err := description.Validate(); !errors.Is(err, ErrStoreDescription) {
			t.Fatalf("provider %q error = %v", provider, err)
		}
	}
}

func TestStoreTypedErrorsPreserveSentinels(t *testing.T) {
	if err := (&StoreNotFoundError{Record: StoreRecordTask, Key: "task-a"}); !errors.Is(err, ErrStoreRecordNotFound) {
		t.Fatalf("not-found error lost sentinel: %v", err)
	}
	if err := (&StoreConflictError{Constraint: StoreConstraintTaskKey, Key: "task-a"}); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("conflict error lost sentinel: %v", err)
	}
	cause := errors.New("connection dropped during commit")
	err := &StoreCommitUnknownError{Cause: cause}
	if !errors.Is(err, ErrStoreCommitUnknown) || !errors.Is(err, cause) {
		t.Fatalf("commit-unknown error lost classification: %v", err)
	}
}
