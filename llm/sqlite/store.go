package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

const maxScanLimit = 4096

func (*store) Description() llm.StoreDescription {
	return llm.StoreDescription{
		Contract: framework.Contract{ID: llm.StoreContractID, Major: llm.StoreContractMajor},
		Provider: "sqlite",
		Version:  fmt.Sprintf("schema-%d", schemaVersion),
	}
}

func (store *store) View(ctx context.Context, callback func(llm.StoreView) error) error {
	if err := store.validateOperation(ctx, callback != nil); err != nil {
		return err
	}
	transaction, err := store.database.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("begin HumanLLM Store view: %w", err)
	}
	unit := newUnit(ctx, transaction)
	defer func() {
		unit.active.Store(false)
		_ = transaction.Rollback()
	}()
	callbackErr := callback(&view{unit: unit})
	unit.active.Store(false)
	if callbackErr != nil {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return errors.Join(callbackErr, fmt.Errorf("rollback HumanLLM Store view: %w", rollbackErr))
		}
		return callbackErr
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit HumanLLM Store view: %w", err)
	}
	return nil
}

func (store *store) Update(ctx context.Context, callback func(llm.StoreTx) error) error {
	if err := store.validateOperation(ctx, callback != nil); err != nil {
		return err
	}
	transaction, err := store.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin HumanLLM Store update: %w", err)
	}
	unit := newUnit(ctx, transaction)
	defer func() {
		unit.active.Store(false)
		_ = transaction.Rollback()
	}()
	base := &view{unit: unit}
	callbackErr := callback(&tx{view: base})
	unit.active.Store(false)
	if callbackErr != nil {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return errors.Join(callbackErr, fmt.Errorf("rollback HumanLLM Store update: %w", rollbackErr))
		}
		return callbackErr
	}
	if err := transaction.Commit(); err != nil {
		return &llm.StoreCommitUnknownError{Cause: err}
	}
	return nil
}

func (store *store) validateOperation(ctx context.Context, hasCallback bool) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", llm.ErrStoreInvalidArgument)
	}
	if !hasCallback {
		return fmt.Errorf("%w: callback is required", llm.ErrStoreInvalidArgument)
	}
	if store == nil || store.database == nil || store.closed.Load() {
		return llm.ErrStoreClosed
	}
	return nil
}

type unit struct {
	ctx    context.Context
	tx     *sql.Tx
	active atomic.Bool
}

func newUnit(ctx context.Context, transaction *sql.Tx) *unit {
	unit := &unit{ctx: ctx, tx: transaction}
	unit.active.Store(true)
	return unit
}

func (unit *unit) ensureActive() error {
	if unit == nil || unit.tx == nil || !unit.active.Load() {
		return llm.ErrStoreClosed
	}
	return nil
}

type view struct{ unit *unit }
type tx struct{ *view }

var _ llm.StoreView = (*view)(nil)
var _ llm.StoreTx = (*tx)(nil)

func invalidArgument(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", llm.ErrStoreInvalidArgument, fmt.Sprintf(format, arguments...))
}

func notFound(kind llm.StoreRecordKind, key any) error {
	return &llm.StoreNotFoundError{Record: kind, Key: fmt.Sprint(key)}
}

func conflict(constraint llm.StoreConstraint, key any) error {
	return &llm.StoreConflictError{Constraint: constraint, Key: fmt.Sprint(key)}
}

func validateReadLimit(kind llm.StoreRecordKind, limit llm.StoreReadLimit) error {
	if limit.MaxBytes < 1 {
		return &llm.StoreLimitError{Record: kind, Limit: limit.MaxBytes}
	}
	return nil
}

func validateScanLimit(limit int) error {
	if limit < 1 || limit > maxScanLimit {
		return invalidArgument("scan limit must be 1..%d", maxScanLimit)
	}
	return nil
}
