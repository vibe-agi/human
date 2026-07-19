package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/vibe-agi/human/llm"
	"github.com/vibe-agi/human/llm/workerws"
)

// The WebSocket adapter's hard inbound ceiling is 64 MiB. Keeping the same
// at-rest ceiling admits every transport-valid delivery while bounding strict
// decoding of a damaged database.
const (
	maxJournalPayloadBytes          = 64 << 20
	maxJournalRejectionPayloadBytes = maxJournalPayloadBytes + 64<<10
)

type rowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (journal *journal) Bind(ctx context.Context, binding workerws.JournalBinding) error {
	return journal.update(ctx, func(tx *sql.Tx) error {
		if err := binding.Validate(); err != nil {
			return errors.Join(workerws.ErrJournalCorrupt, err)
		}
		current, bound, err := inspectBinding(ctx, tx)
		if err != nil {
			return err
		}
		if bound {
			if current != binding {
				return workerws.ErrJournalConflict
			}
			return nil
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE llm_worker_journal_meta
			SET binding_gateway_id = ?, binding_worker_id = ?
			WHERE singleton = 1
			  AND binding_gateway_id IS NULL
			  AND binding_worker_id IS NULL`,
			string(binding.Gateway), string(binding.Worker),
		)
		if err != nil {
			return fmt.Errorf("persist HumanLLM worker Journal binding: %w", err)
		}
		if count, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("inspect HumanLLM worker Journal binding update: %w", err)
		} else if count != 1 {
			return errors.Join(workerws.ErrJournalCorrupt, errors.New("binding metadata changed inside a serializable transaction"))
		}
		return nil
	})
}

func (journal *journal) PutAssignment(
	ctx context.Context,
	record workerws.JournalAssignment,
) (workerws.JournalEntryState, error) {
	var state workerws.JournalEntryState
	err := journal.update(ctx, func(tx *sql.Tx) error {
		binding, err := requireBinding(ctx, tx)
		if err != nil {
			return err
		}
		if err := validateAssignmentRecord(record, binding); err != nil {
			return err
		}

		var storedDigest string
		var storedState string
		err = tx.QueryRowContext(ctx, `
			SELECT digest, state
			FROM llm_worker_journal_assignments
			WHERE delivery_id = ?`, string(record.Delivery.ID),
		).Scan(&storedDigest, &storedState)
		switch {
		case err == nil:
			state, err = exactState(workerws.JournalDigest(storedDigest), storedState, record.Digest)
			return err
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("load HumanLLM worker Journal assignment: %w", err)
		}

		payload, err := encodePayload(record.Delivery)
		if err != nil {
			return err
		}
		if err := journal.requirePendingCapacity(ctx, tx, int64(len(payload))); err != nil {
			return err
		}
		sequence, err := allocateSequence(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO llm_worker_journal_assignments (
			  delivery_id, sequence, digest, state, payload
			) VALUES (?, ?, ?, 'pending', ?)`,
			string(record.Delivery.ID), int64(sequence), string(record.Digest), payload,
		); err != nil {
			return fmt.Errorf("insert HumanLLM worker Journal assignment: %w", err)
		}
		state = workerws.JournalEntryPending
		return nil
	})
	return state, err
}

func (journal *journal) ConfirmAssignment(ctx context.Context, id llm.WorkerDeliveryID) error {
	return journal.update(ctx, func(tx *sql.Tx) error {
		if _, err := requireBinding(ctx, tx); err != nil {
			return err
		}
		var state string
		if err := tx.QueryRowContext(ctx, `
			SELECT state
			FROM llm_worker_journal_assignments
			WHERE delivery_id = ?`, string(id),
		).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return workerws.ErrJournalNotFound
			}
			return fmt.Errorf("load HumanLLM worker Journal assignment settlement: %w", err)
		}
		switch workerws.JournalEntryState(state) {
		case workerws.JournalEntrySettled:
			return nil
		case workerws.JournalEntryPending:
		default:
			return corruptf("assignment %q has invalid state %q", id, state)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE llm_worker_journal_assignments
			SET state = 'settled', payload = NULL
			WHERE delivery_id = ? AND state = 'pending'`, string(id),
		)
		if err != nil {
			return fmt.Errorf("settle HumanLLM worker Journal assignment: %w", err)
		}
		return requireOneRow(result, "assignment settlement")
	})
}

func (journal *journal) ListAssignments(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalAssignment, error) {
	release, err := journal.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	binding, err := requireBinding(ctx, journal.database)
	if err != nil {
		return nil, err
	}
	if err := validatePage(after, limit); err != nil {
		return nil, err
	}
	rows, err := journal.database.QueryContext(ctx, `
		SELECT delivery_id, sequence, digest, payload
		FROM llm_worker_journal_assignments
		WHERE state = 'pending' AND sequence > ?
		ORDER BY sequence
		LIMIT ?`, int64(after), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list HumanLLM worker Journal assignments: %w", err)
	}
	defer rows.Close()
	result := make([]workerws.JournalAssignment, 0)
	for rows.Next() {
		var deliveryID string
		var sequence int64
		var digest string
		var payload []byte
		if err := rows.Scan(&deliveryID, &sequence, &digest, &payload); err != nil {
			return nil, fmt.Errorf("scan HumanLLM worker Journal assignment: %w", err)
		}
		var delivery llm.WorkerAssignmentDelivery
		if err := decodePayload(payload, &delivery); err != nil {
			return nil, err
		}
		record := workerws.JournalAssignment{
			Sequence: workerws.JournalSequence(sequence),
			Digest:   workerws.JournalDigest(digest), Delivery: delivery,
		}
		if string(record.Delivery.ID) != deliveryID {
			return nil, corruptf("assignment row identity %q does not match payload %q", deliveryID, record.Delivery.ID)
		}
		if err := validateStoredAssignment(record, binding); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HumanLLM worker Journal assignments: %w", err)
	}
	return result, nil
}

func (journal *journal) PutEvent(
	ctx context.Context,
	record workerws.JournalEvent,
) (workerws.JournalEntryState, error) {
	var state workerws.JournalEntryState
	err := journal.update(ctx, func(tx *sql.Tx) error {
		binding, err := requireBinding(ctx, tx)
		if err != nil {
			return err
		}
		if err := validateEventRecord(record, binding); err != nil {
			return err
		}

		var storedDigest string
		var storedState string
		err = tx.QueryRowContext(ctx, `
			SELECT digest, state
			FROM llm_worker_journal_events
			WHERE delivery_id = ?`, string(record.Delivery.ID),
		).Scan(&storedDigest, &storedState)
		switch {
		case err == nil:
			state, err = exactState(workerws.JournalDigest(storedDigest), storedState, record.Digest)
			return err
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("load HumanLLM worker Journal event: %w", err)
		}

		payload, err := encodePayload(record.Delivery)
		if err != nil {
			return err
		}
		if err := journal.requirePendingCapacity(ctx, tx, int64(len(payload))); err != nil {
			return err
		}
		sequence, err := allocateSequence(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO llm_worker_journal_events (
			  delivery_id, sequence, digest, state, payload, receipt_digest
			) VALUES (?, ?, ?, 'pending', ?, NULL)`,
			string(record.Delivery.ID), int64(sequence), string(record.Digest), payload,
		); err != nil {
			return fmt.Errorf("insert HumanLLM worker Journal event: %w", err)
		}
		state = workerws.JournalEntryPending
		return nil
	})
	return state, err
}

func (journal *journal) ListEvents(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalEvent, error) {
	release, err := journal.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	binding, err := requireBinding(ctx, journal.database)
	if err != nil {
		return nil, err
	}
	if err := validatePage(after, limit); err != nil {
		return nil, err
	}
	rows, err := journal.database.QueryContext(ctx, `
		SELECT delivery_id, sequence, digest, payload
		FROM llm_worker_journal_events
		WHERE state = 'pending' AND sequence > ?
		ORDER BY sequence
		LIMIT ?`, int64(after), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list HumanLLM worker Journal events: %w", err)
	}
	defer rows.Close()
	result := make([]workerws.JournalEvent, 0)
	for rows.Next() {
		var deliveryID string
		var sequence int64
		var digest string
		var payload []byte
		if err := rows.Scan(&deliveryID, &sequence, &digest, &payload); err != nil {
			return nil, fmt.Errorf("scan HumanLLM worker Journal event: %w", err)
		}
		var delivery llm.WorkerEventDelivery
		if err := decodePayload(payload, &delivery); err != nil {
			return nil, err
		}
		record := workerws.JournalEvent{
			Sequence: workerws.JournalSequence(sequence),
			Digest:   workerws.JournalDigest(digest), Delivery: delivery,
		}
		if string(record.Delivery.ID) != deliveryID {
			return nil, corruptf("event row identity %q does not match payload %q", deliveryID, record.Delivery.ID)
		}
		if err := validateStoredEvent(record, binding); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HumanLLM worker Journal events: %w", err)
	}
	return result, nil
}

func (journal *journal) SettleEvent(
	ctx context.Context,
	receipt llm.WorkerEventReceipt,
	eventDigest workerws.JournalDigest,
	receiptDigest workerws.JournalDigest,
) error {
	return journal.update(ctx, func(tx *sql.Tx) error {
		binding, err := requireBinding(ctx, tx)
		if err != nil {
			return err
		}
		if err := eventDigest.Validate(); err != nil {
			return err
		}
		if err := receiptDigest.Validate(); err != nil {
			return err
		}
		var storedEventDigest string
		var state string
		var payload []byte
		var storedReceiptDigest sql.NullString
		err = tx.QueryRowContext(ctx, `
			SELECT digest, state, payload, receipt_digest
			FROM llm_worker_journal_events
			WHERE delivery_id = ?`, string(receipt.Delivery),
		).Scan(&storedEventDigest, &state, &payload, &storedReceiptDigest)
		if errors.Is(err, sql.ErrNoRows) {
			return workerws.ErrJournalNotFound
		}
		if err != nil {
			return fmt.Errorf("load HumanLLM worker Journal event settlement: %w", err)
		}
		storedEvent := workerws.JournalDigest(storedEventDigest)
		if err := storedEvent.Validate(); err != nil {
			return err
		}
		if storedEvent != eventDigest {
			return workerws.ErrJournalConflict
		}
		switch workerws.JournalEntryState(state) {
		case workerws.JournalEntrySettled:
			if !storedReceiptDigest.Valid {
				return corruptf("settled event %q has no receipt digest", receipt.Delivery)
			}
			storedReceipt := workerws.JournalDigest(storedReceiptDigest.String)
			if err := storedReceipt.Validate(); err != nil {
				return err
			}
			if storedReceipt != receiptDigest {
				return workerws.ErrJournalConflict
			}
			return nil
		case workerws.JournalEntryPending:
			if storedReceiptDigest.Valid || payload == nil {
				return corruptf("pending event %q has invalid settlement fields", receipt.Delivery)
			}
		default:
			return corruptf("event %q has invalid state %q", receipt.Delivery, state)
		}

		var delivery llm.WorkerEventDelivery
		if err := decodePayload(payload, &delivery); err != nil {
			return err
		}
		if err := validateEventDelivery(delivery, binding); err != nil {
			return err
		}
		if err := receipt.ValidateFor(delivery); err != nil {
			return errors.Join(workerws.ErrJournalCorrupt, err)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE llm_worker_journal_events
			SET state = 'settled', payload = NULL, receipt_digest = ?
			WHERE delivery_id = ? AND state = 'pending' AND digest = ?`,
			string(receiptDigest), string(receipt.Delivery), string(eventDigest),
		)
		if err != nil {
			return fmt.Errorf("settle HumanLLM worker Journal event: %w", err)
		}
		if err := requireOneRow(result, "event settlement"); err != nil {
			return err
		}

		if receipt.Decision == llm.WorkerEventNACK {
			sequence, err := allocateSequence(ctx, tx)
			if err != nil {
				return err
			}
			rejected := workerws.RejectedEvent{Delivery: delivery, Receipt: receipt}
			rejectedPayload, err := encodePayloadLimit(rejected, maxJournalRejectionPayloadBytes)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO llm_worker_journal_rejections (
				  delivery_id, sequence, event_digest, receipt_digest, state, payload
				) VALUES (?, ?, ?, ?, 'pending', ?)`,
				string(receipt.Delivery), int64(sequence), string(eventDigest),
				string(receiptDigest), rejectedPayload,
			); err != nil {
				return fmt.Errorf("insert HumanLLM worker Journal rejection: %w", err)
			}
		}
		return nil
	})
}

func (journal *journal) ListRejections(
	ctx context.Context,
	after workerws.JournalSequence,
	limit int,
) ([]workerws.JournalRejection, error) {
	release, err := journal.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	binding, err := requireBinding(ctx, journal.database)
	if err != nil {
		return nil, err
	}
	if err := validatePage(after, limit); err != nil {
		return nil, err
	}
	rows, err := journal.database.QueryContext(ctx, `
		SELECT delivery_id, sequence, event_digest, receipt_digest, payload
		FROM llm_worker_journal_rejections
		WHERE state = 'pending' AND sequence > ?
		ORDER BY sequence
		LIMIT ?`, int64(after), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list HumanLLM worker Journal rejections: %w", err)
	}
	defer rows.Close()
	result := make([]workerws.JournalRejection, 0)
	for rows.Next() {
		var deliveryID string
		var sequence int64
		var eventDigest string
		var receiptDigest string
		var payload []byte
		if err := rows.Scan(&deliveryID, &sequence, &eventDigest, &receiptDigest, &payload); err != nil {
			return nil, fmt.Errorf("scan HumanLLM worker Journal rejection: %w", err)
		}
		var rejected workerws.RejectedEvent
		if err := decodePayload(payload, &rejected); err != nil {
			return nil, err
		}
		record := workerws.JournalRejection{
			Sequence:      workerws.JournalSequence(sequence),
			EventDigest:   workerws.JournalDigest(eventDigest),
			ReceiptDigest: workerws.JournalDigest(receiptDigest),
			RejectedEvent: rejected,
		}
		if string(record.Delivery.ID) != deliveryID {
			return nil, corruptf("rejection row identity %q does not match payload %q", deliveryID, record.Delivery.ID)
		}
		if err := validateStoredRejection(record, binding); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate HumanLLM worker Journal rejections: %w", err)
	}
	return result, nil
}

func (journal *journal) ConfirmRejection(ctx context.Context, id llm.WorkerDeliveryID) error {
	return journal.update(ctx, func(tx *sql.Tx) error {
		if _, err := requireBinding(ctx, tx); err != nil {
			return err
		}
		var state string
		if err := tx.QueryRowContext(ctx, `
			SELECT state
			FROM llm_worker_journal_rejections
			WHERE delivery_id = ?`, string(id),
		).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return workerws.ErrJournalNotFound
			}
			return fmt.Errorf("load HumanLLM worker Journal rejection settlement: %w", err)
		}
		switch workerws.JournalEntryState(state) {
		case workerws.JournalEntrySettled:
			return nil
		case workerws.JournalEntryPending:
		default:
			return corruptf("rejection %q has invalid state %q", id, state)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE llm_worker_journal_rejections
			SET state = 'settled', payload = NULL
			WHERE delivery_id = ? AND state = 'pending'`, string(id),
		)
		if err != nil {
			return fmt.Errorf("settle HumanLLM worker Journal rejection: %w", err)
		}
		return requireOneRow(result, "rejection settlement")
	})
}

func inspectBinding(ctx context.Context, query rowQuerier) (workerws.JournalBinding, bool, error) {
	var gateway sql.NullString
	var worker sql.NullString
	if err := query.QueryRowContext(ctx, `
		SELECT binding_gateway_id, binding_worker_id
		FROM llm_worker_journal_meta
		WHERE singleton = 1`).Scan(&gateway, &worker); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workerws.JournalBinding{}, false, corruptf("binding metadata is missing")
		}
		return workerws.JournalBinding{}, false, fmt.Errorf("load HumanLLM worker Journal binding metadata: %w", err)
	}
	if !gateway.Valid && !worker.Valid {
		return workerws.JournalBinding{}, false, nil
	}
	if !gateway.Valid || !worker.Valid {
		return workerws.JournalBinding{}, false, corruptf("binding metadata is only partially populated")
	}
	binding := workerws.JournalBinding{
		Gateway: workerws.GatewayID(gateway.String),
		Worker:  llm.WorkerID(worker.String),
	}
	if err := binding.Validate(); err != nil {
		return workerws.JournalBinding{}, false, errors.Join(workerws.ErrJournalCorrupt, err)
	}
	return binding, true, nil
}

func requireBinding(ctx context.Context, query rowQuerier) (workerws.JournalBinding, error) {
	binding, bound, err := inspectBinding(ctx, query)
	if err != nil {
		return workerws.JournalBinding{}, err
	}
	if !bound {
		return workerws.JournalBinding{}, errors.Join(workerws.ErrJournalCorrupt, errors.New("HumanLLM worker Journal is not bound"))
	}
	return binding, nil
}

func allocateSequence(ctx context.Context, tx *sql.Tx) (workerws.JournalSequence, error) {
	var current int64
	if err := tx.QueryRowContext(ctx, `
		SELECT next_sequence
		FROM llm_worker_journal_meta
		WHERE singleton = 1`).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, corruptf("sequence allocator metadata is missing")
		}
		return 0, fmt.Errorf("load HumanLLM worker Journal sequence allocator: %w", err)
	}
	if current < 0 || current >= math.MaxInt64 {
		return 0, errors.Join(workerws.ErrJournalLimit, errors.New("HumanLLM worker Journal sequence space is exhausted"))
	}
	next := current + 1
	result, err := tx.ExecContext(ctx, `
		UPDATE llm_worker_journal_meta
		SET next_sequence = ?
		WHERE singleton = 1 AND next_sequence = ?`, next, current,
	)
	if err != nil {
		return 0, fmt.Errorf("advance HumanLLM worker Journal sequence: %w", err)
	}
	if err := requireOneRow(result, "sequence allocation"); err != nil {
		return 0, err
	}
	return workerws.JournalSequence(next), nil
}

// requirePendingCapacity measures encoded payload bytes, not an individual
// canonical field. It runs only for a genuinely new assignment/event and
// inside the same serializable transaction as insertion. Settlement and
// compaction deliberately bypass it so an over-budget Journal can converge.
func (journal *journal) requirePendingCapacity(ctx context.Context, tx *sql.Tx, newPayloadBytes int64) error {
	var records int64
	var payloadBytes int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0)
		FROM (
		  SELECT length(payload) AS payload_bytes
		  FROM llm_worker_journal_assignments
		  WHERE state = 'pending'
		  UNION ALL
		  SELECT length(payload) AS payload_bytes
		  FROM llm_worker_journal_events
		  WHERE state = 'pending'
		  UNION ALL
		  SELECT length(payload) AS payload_bytes
		  FROM llm_worker_journal_rejections
		  WHERE state = 'pending'
		)`).Scan(&records, &payloadBytes); err != nil {
		return fmt.Errorf("measure HumanLLM worker Journal pending payloads: %w", err)
	}
	if records < 0 || payloadBytes < 0 || newPayloadBytes < 0 {
		return corruptf("pending payload accounting is negative")
	}
	if records >= journal.maxPendingRecords {
		return fmt.Errorf(
			"%w: %d pending records already meet the configured maximum %d",
			workerws.ErrJournalLimit, records, journal.maxPendingRecords,
		)
	}
	if newPayloadBytes > journal.maxPendingBytes || payloadBytes > journal.maxPendingBytes-newPayloadBytes {
		return fmt.Errorf(
			"%w: pending payload bytes would grow from %d to %d above the configured maximum %d",
			workerws.ErrJournalLimit, payloadBytes, payloadBytes+newPayloadBytes, journal.maxPendingBytes,
		)
	}
	return nil
}

func exactState(
	storedDigest workerws.JournalDigest,
	storedState string,
	wantDigest workerws.JournalDigest,
) (workerws.JournalEntryState, error) {
	if err := storedDigest.Validate(); err != nil {
		return "", err
	}
	if storedDigest != wantDigest {
		return "", workerws.ErrJournalConflict
	}
	state := workerws.JournalEntryState(storedState)
	switch state {
	case workerws.JournalEntryPending, workerws.JournalEntrySettled:
		return state, nil
	default:
		return "", corruptf("record has invalid state %q", storedState)
	}
}

func validateAssignmentRecord(record workerws.JournalAssignment, binding workerws.JournalBinding) error {
	if record.Sequence != 0 {
		return corruptf("new assignment supplies sequence %d", record.Sequence)
	}
	if err := record.Digest.Validate(); err != nil {
		return err
	}
	return validateAssignmentDelivery(record.Delivery, binding)
}

func validateStoredAssignment(record workerws.JournalAssignment, binding workerws.JournalBinding) error {
	if record.Sequence == 0 || record.Sequence > workerws.JournalSequence(math.MaxInt64) {
		return corruptf("stored assignment has invalid sequence %d", record.Sequence)
	}
	if err := record.Digest.Validate(); err != nil {
		return err
	}
	return validateAssignmentDelivery(record.Delivery, binding)
}

func validateAssignmentDelivery(delivery llm.WorkerAssignmentDelivery, binding workerws.JournalBinding) error {
	err := delivery.ValidateFor(llm.AuthenticatedWorker{
		WorkerID: binding.Worker, SessionID: "journal-validation",
	})
	if err != nil {
		return errors.Join(workerws.ErrJournalCorrupt, err)
	}
	return nil
}

func validateEventRecord(record workerws.JournalEvent, binding workerws.JournalBinding) error {
	if record.Sequence != 0 {
		return corruptf("new event supplies sequence %d", record.Sequence)
	}
	if err := record.Digest.Validate(); err != nil {
		return err
	}
	return validateEventDelivery(record.Delivery, binding)
}

func validateStoredEvent(record workerws.JournalEvent, binding workerws.JournalBinding) error {
	if record.Sequence == 0 || record.Sequence > workerws.JournalSequence(math.MaxInt64) {
		return corruptf("stored event has invalid sequence %d", record.Sequence)
	}
	if err := record.Digest.Validate(); err != nil {
		return err
	}
	return validateEventDelivery(record.Delivery, binding)
}

func validateEventDelivery(delivery llm.WorkerEventDelivery, binding workerws.JournalBinding) error {
	err := delivery.ValidateFor(llm.AuthenticatedWorker{
		WorkerID: binding.Worker, SessionID: "journal-validation",
	})
	if err != nil {
		return errors.Join(workerws.ErrJournalCorrupt, err)
	}
	return nil
}

func validateStoredRejection(record workerws.JournalRejection, binding workerws.JournalBinding) error {
	if record.Sequence == 0 || record.Sequence > workerws.JournalSequence(math.MaxInt64) {
		return corruptf("stored rejection has invalid sequence %d", record.Sequence)
	}
	if err := record.EventDigest.Validate(); err != nil {
		return err
	}
	if err := record.ReceiptDigest.Validate(); err != nil {
		return err
	}
	if err := validateEventDelivery(record.Delivery, binding); err != nil {
		return err
	}
	if err := record.Receipt.ValidateFor(record.Delivery); err != nil {
		return errors.Join(workerws.ErrJournalCorrupt, err)
	}
	return nil
}

func validatePage(after workerws.JournalSequence, limit int) error {
	if after > workerws.JournalSequence(math.MaxInt64) {
		return fmt.Errorf("%w: cursor exceeds MaxInt64", workerws.ErrJournalLimit)
	}
	if limit < 1 || limit > workerws.MaxJournalPageSize {
		return fmt.Errorf("%w: page limit must be 1..%d", workerws.ErrJournalLimit, workerws.MaxJournalPageSize)
	}
	return nil
}

func encodePayload(value any) ([]byte, error) {
	return encodePayloadLimit(value, maxJournalPayloadBytes)
}

func encodePayloadLimit(value any, limit int) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Join(workerws.ErrJournalCorrupt, fmt.Errorf("encode Journal payload: %w", err))
	}
	if len(payload) == 0 || len(payload) > limit {
		return nil, errors.Join(workerws.ErrJournalLimit, fmt.Errorf("Journal payload is %d bytes", len(payload)))
	}
	return payload, nil
}

func decodePayload(payload []byte, destination any) error {
	if len(payload) == 0 || len(payload) > maxJournalRejectionPayloadBytes {
		return corruptf("stored payload size %d is invalid", len(payload))
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return corruptf("decode stored payload: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return corruptf("stored payload contains more than one JSON value")
		}
		return corruptf("decode stored payload trailer: %v", err)
	}
	return nil
}

func requireOneRow(result sql.Result, operation string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect HumanLLM worker Journal %s: %w", operation, err)
	}
	if count != 1 {
		return corruptf("%s affected %d rows, want 1", operation, count)
	}
	return nil
}

func corruptf(format string, arguments ...any) error {
	return errors.Join(workerws.ErrJournalCorrupt, fmt.Errorf(format, arguments...))
}
