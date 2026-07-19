package agent

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vibe-agi/human/framework"
)

// sqliteStore is the unexported physical implementation shared by Agent's
// DatabasePath composition and the official agent/sqlite adapter. Keeping the
// implementation here lets the adapter depend on the public Store contract
// without exporting a database handle or creating a package cycle.
type sqliteStore struct {
	database *sql.DB
	closed   atomic.Bool
}

// newSQLiteStore does not take ownership of database. The resource that opened
// the database remains solely responsible for closing it.
func newSQLiteStore(database *sql.DB) *sqliteStore {
	return &sqliteStore{database: database}
}

// close stops new operations before the owned database is released. Store has
// no public lifecycle method: only the framework.Resource created by the
// SQLite composition primitive calls this hook.
func (store *sqliteStore) close() {
	if store != nil {
		store.closed.Store(true)
	}
}

func (*sqliteStore) Description() StoreDescription {
	return StoreDescription{
		Contract: framework.Contract{ID: StoreContractID, Major: StoreContractMajor},
		Provider: "sqlite",
		Version:  fmt.Sprintf("schema-%d", agentSchemaVersion),
	}
}

func (store *sqliteStore) View(ctx context.Context, callback func(StoreView) error) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	if callback == nil {
		return fmt.Errorf("%w: Store View callback is required", ErrInvalidArgument)
	}
	if store == nil || store.database == nil || store.closed.Load() {
		return ErrStoreClosed
	}
	tx, err := store.database.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("begin Agent Store view: %w", err)
	}
	state := newSQLiteStoreUnit(ctx, tx)
	defer func() {
		state.active.Store(false)
		_ = tx.Rollback()
	}()
	view := &sqliteStoreView{unit: state}
	callbackErr := callback(view)
	state.active.Store(false)
	if callbackErr != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return errors.Join(callbackErr, fmt.Errorf("rollback Agent Store view: %w", rollbackErr))
		}
		return callbackErr
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Agent Store view: %w", err)
	}
	return nil
}

func (store *sqliteStore) Update(ctx context.Context, callback func(StoreTx) error) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	if callback == nil {
		return fmt.Errorf("%w: Store Update callback is required", ErrInvalidArgument)
	}
	if store == nil || store.database == nil || store.closed.Load() {
		return ErrStoreClosed
	}
	tx, err := store.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin Agent Store update: %w", err)
	}
	state := newSQLiteStoreUnit(ctx, tx)
	defer func() {
		state.active.Store(false)
		_ = tx.Rollback()
	}()
	view := &sqliteStoreView{unit: state}
	unit := &sqliteStoreTx{sqliteStoreView: view}
	callbackErr := callback(unit)
	state.active.Store(false)
	if callbackErr != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return errors.Join(callbackErr, fmt.Errorf("rollback Agent Store update: %w", rollbackErr))
		}
		return callbackErr
	}
	if err := tx.Commit(); err != nil {
		// database/sql does not expose enough information to distinguish every
		// transport-level Commit failure. Conservatively force exact command retry.
		return &StoreCommitUnknownError{Cause: err}
	}
	return nil
}

type sqliteStoreUnit struct {
	ctx    context.Context
	tx     *sql.Tx
	active atomic.Bool
}

func newSQLiteStoreUnit(ctx context.Context, tx *sql.Tx) *sqliteStoreUnit {
	unit := &sqliteStoreUnit{ctx: ctx, tx: tx}
	unit.active.Store(true)
	return unit
}

func (unit *sqliteStoreUnit) ensureActive() error {
	if unit == nil || unit.tx == nil || !unit.active.Load() {
		return ErrStoreClosed
	}
	return nil
}

type sqliteStoreView struct {
	unit *sqliteStoreUnit
}

type sqliteStoreTx struct {
	*sqliteStoreView
}

var _ Store = (*sqliteStore)(nil)
var _ StoreView = (*sqliteStoreView)(nil)
var _ StoreTx = (*sqliteStoreTx)(nil)

func validateSQLiteReadLimit(record StoreRecordKind, limit StoreReadLimit) error {
	if limit.MaxBytes < 1 {
		return &StoreLimitError{Record: record, Limit: limit.MaxBytes}
	}
	return nil
}

func validateSQLiteScanLimit(limit int) error {
	if limit < 1 || limit > MaxPageSize+1 {
		return fmt.Errorf("%w: Store scan limit must be 1..%d", ErrInvalidArgument, MaxPageSize+1)
	}
	return nil
}

func sqliteNotFound(record StoreRecordKind, key any) error {
	return &StoreNotFoundError{Record: record, Key: fmt.Sprint(key)}
}

func sqliteConflict(constraint StoreConstraint, key any) error {
	return &StoreConflictError{Constraint: constraint, Key: fmt.Sprint(key)}
}

func (view *sqliteStoreView) LookupCommand(
	key StoreCommandKey,
	limit StoreReadLimit,
) (StoreCommandRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreCommandRecord{}, err
	}
	if err := validateSQLiteReadLimit(StoreRecordCommand, limit); err != nil {
		return StoreCommandRecord{}, err
	}
	var record StoreCommandRecord
	var result []byte
	var size int64
	var created int64
	record.Authority = key.Authority
	record.ID = key.ID
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT kind, digest, result_kind,
		       CASE WHEN length(result) <= ? THEN result ELSE NULL END,
		       length(result), result_digest, created_at
		FROM agent_commands
		WHERE authority_id = ? AND command_id = ?`,
		limit.MaxBytes, key.Authority, key.ID,
	).Scan(
		&record.Kind, &record.IntentDigest, &record.ResultKind, &result,
		&size, &record.ResultDigest, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreCommandRecord{}, sqliteNotFound(StoreRecordCommand, key.ID)
	}
	if err != nil {
		return StoreCommandRecord{}, fmt.Errorf("load Agent Store command: %w", err)
	}
	if size > limit.MaxBytes || result == nil {
		return StoreCommandRecord{}, &StoreLimitError{Record: StoreRecordCommand, Limit: limit.MaxBytes}
	}
	record.Result = bytes.Clone(result)
	record.CreatedAt = fromUnixNano(created)
	return cloneStoreCommandRecord(record), nil
}

func (view *sqliteStoreView) LoadTask(ref TaskRef) (StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreTaskRecord{}, err
	}
	return loadSQLiteStoreTask(view.unit.ctx, view.unit.tx, ref)
}

// loadSQLiteStoreTask materializes physical columns without applying Agent
// state-machine or digest policy. Store is the persistence port; the Agent core
// validates the returned aggregate after the callback. Keeping that validation
// out of the adapter is also required for read-your-writes while a transaction
// is atomically linking an Artifact and advancing its Task.
func loadSQLiteStoreTask(ctx context.Context, tx *sql.Tx, ref TaskRef) (StoreTaskRecord, error) {
	var record StoreTaskRecord
	record.Task.Ref = ref
	record.Task.Context.Authority = ref.Workspace.Authority
	var created, updated int64
	var artifactID, artifactState sql.NullString
	var submissionID, finalMessageID, submissionArtifactID sql.NullString
	var publishedAt sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT t.context_id, t.state, t.revision, t.message_count, t.event_count,
		       t.created_at, t.updated_at, t.lease_owner, t.lease_fence,
		       a.artifact_id, a.state,
		       s.submission_id, s.final_message_id, s.artifact_id, s.published_at
		FROM agent_tasks AS t
		LEFT JOIN agent_artifacts AS a
		  ON a.authority_id = t.authority_id
		 AND a.workspace_id = t.workspace_id
		 AND a.task_id = t.task_id
		LEFT JOIN agent_submissions AS s
		  ON s.authority_id = t.authority_id
		 AND s.workspace_id = t.workspace_id
		 AND s.task_id = t.task_id
		WHERE t.authority_id = ? AND t.workspace_id = ? AND t.task_id = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(
		&record.Task.Context.ID, &record.Task.State, &record.Task.Revision,
		&record.Task.MessageCount, &record.Task.EventCount, &created, &updated,
		&record.Lease.Owner, &record.Lease.Fence, &artifactID, &artifactState,
		&submissionID, &finalMessageID, &submissionArtifactID, &publishedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreTaskRecord{}, sqliteNotFound(StoreRecordTask, ref.ID)
	}
	if err != nil {
		return StoreTaskRecord{}, fmt.Errorf("load Agent Store task: %w", err)
	}
	record.Task.CreatedAt = fromUnixNano(created)
	record.Task.UpdatedAt = fromUnixNano(updated)
	if artifactID.Valid != artifactState.Valid {
		return StoreTaskRecord{}, fmt.Errorf("%w: partial Artifact columns for task %q", ErrCorruptStore, ref.ID)
	}
	if artifactID.Valid {
		artifactRef := ArtifactRef{Workspace: ref.Workspace, ID: ArtifactID(artifactID.String)}
		state := ArtifactState(artifactState.String)
		record.Task.Artifact = &artifactRef
		record.ArtifactState = &state
	}
	if submissionID.Valid || finalMessageID.Valid || publishedAt.Valid {
		if !submissionID.Valid || !finalMessageID.Valid || !publishedAt.Valid {
			return StoreTaskRecord{}, fmt.Errorf("%w: partial submission columns for task %q", ErrCorruptStore, ref.ID)
		}
		record.Task.Submission = &Submission{
			ID:           SubmissionID(submissionID.String),
			Task:         ref,
			FinalMessage: MessageID(finalMessageID.String),
			PublishedAt:  fromUnixNano(publishedAt.Int64),
		}
		if submissionArtifactID.Valid {
			artifactRef := ArtifactRef{Workspace: ref.Workspace, ID: ArtifactID(submissionArtifactID.String)}
			record.Task.Submission.Artifact = &artifactRef
		}
	} else if submissionArtifactID.Valid {
		return StoreTaskRecord{}, fmt.Errorf("%w: orphaned submission Artifact for task %q", ErrCorruptStore, ref.ID)
	}
	return cloneStoreTaskRecord(record), nil
}

func (view *sqliteStoreView) ResolveTask(authority AuthorityID, id TaskID) (TaskRef, error) {
	if err := view.unit.ensureActive(); err != nil {
		return TaskRef{}, err
	}
	var workspaceID WorkspaceID
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT workspace_id FROM agent_tasks
		WHERE authority_id = ? AND task_id = ?`, authority, id,
	).Scan(&workspaceID)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskRef{}, sqliteNotFound(StoreRecordTask, id)
	}
	if err != nil {
		return TaskRef{}, fmt.Errorf("resolve Agent Store task: %w", err)
	}
	return TaskRef{Workspace: WorkspaceRef{Authority: authority, ID: workspaceID}, ID: id}, nil
}

func (view *sqliteStoreView) LoadMessage(
	key StoreMessageKey,
	limit StoreReadLimit,
) (StoreMessageRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreMessageRecord{}, err
	}
	if err := validateSQLiteReadLimit(StoreRecordMessage, limit); err != nil {
		return StoreMessageRecord{}, err
	}
	var record StoreMessageRecord
	var size int64
	var created int64
	record.ID = key.ID
	record.Task.Workspace.Authority = key.Authority
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT workspace_id, task_id, sequence, author,
		       CASE WHEN length(parts) <= ? THEN parts ELSE NULL END,
		       length(parts), digest, created_at
		FROM agent_messages
		WHERE authority_id = ? AND message_id = ?`,
		limit.MaxBytes, key.Authority, key.ID,
	).Scan(
		&record.Task.Workspace.ID, &record.Task.ID, &record.Sequence, &record.Author,
		&record.EncodedParts, &size, &record.PartsDigest, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreMessageRecord{}, sqliteNotFound(StoreRecordMessage, key.ID)
	}
	if err != nil {
		return StoreMessageRecord{}, fmt.Errorf("load Agent Store message: %w", err)
	}
	if size > limit.MaxBytes || record.EncodedParts == nil {
		return StoreMessageRecord{}, &StoreLimitError{Record: StoreRecordMessage, Limit: limit.MaxBytes}
	}
	record.EncodedParts = bytes.Clone(record.EncodedParts)
	record.CreatedAt = fromUnixNano(created)
	return cloneStoreMessageRecord(record), nil
}

func (view *sqliteStoreView) LoadArtifact(
	ref ArtifactRef,
	limit StoreReadLimit,
) (StoreArtifactRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreArtifactRecord{}, err
	}
	if err := validateSQLiteReadLimit(StoreRecordArtifact, limit); err != nil {
		return StoreArtifactRecord{}, err
	}
	var record StoreArtifactRecord
	var encodedSize, payloadSize int64
	var frozen int64
	var published, discarded sql.NullInt64
	record.Artifact.Ref = ref
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT task_id, state, base_revision, result_revision, artifact_digest,
		       payload_digest, media_type,
		       CASE WHEN length(payload) <= ? THEN payload ELSE NULL END,
		       length(payload), payload_size, frozen_at, published_at, discarded_at
		FROM agent_artifacts
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		limit.MaxBytes, ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(
		&record.Artifact.Task.ID, &record.Artifact.State,
		&record.Artifact.BaseRevision, &record.Artifact.ResultRevision,
		&record.Artifact.Digest, &record.Artifact.PayloadDigest,
		&record.Artifact.MediaType, &record.EncodedPayload, &encodedSize, &payloadSize,
		&frozen, &published, &discarded,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreArtifactRecord{}, sqliteNotFound(StoreRecordArtifact, ref.ID)
	}
	if err != nil {
		return StoreArtifactRecord{}, fmt.Errorf("load Agent Store Artifact: %w", err)
	}
	if encodedSize > limit.MaxBytes || record.EncodedPayload == nil {
		return StoreArtifactRecord{}, &StoreLimitError{Record: StoreRecordArtifact, Limit: limit.MaxBytes}
	}
	record.Artifact.Task.Workspace = ref.Workspace
	record.Artifact.PayloadSize = payloadSize
	record.Artifact.FrozenAt = fromUnixNano(frozen)
	if published.Valid {
		value := fromUnixNano(published.Int64)
		record.Artifact.PublishedAt = &value
	}
	if discarded.Valid {
		value := fromUnixNano(discarded.Int64)
		record.Artifact.DiscardedAt = &value
	}
	return cloneStoreArtifactRecord(record), nil
}

func (view *sqliteStoreView) LoadApplyReceipt(ref ArtifactRef) (StoreApplyReceiptRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreApplyReceiptRecord{}, err
	}
	var receipt ApplyReceipt
	var recorded int64
	receipt.Artifact = ref
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT receipt_id, decision, artifact_digest, base_revision,
		       result_revision, observed_revision, code, message, recorded_at
		FROM agent_apply_receipts
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(
		&receipt.ID, &receipt.Decision, &receipt.ArtifactDigest,
		&receipt.BaseRevision, &receipt.ResultRevision, &receipt.ObservedRevision,
		&receipt.Code, &receipt.Message, &recorded,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreApplyReceiptRecord{}, sqliteNotFound(StoreRecordApplyReceipt, ref.ID)
	}
	if err != nil {
		return StoreApplyReceiptRecord{}, fmt.Errorf("load Agent Store apply receipt: %w", err)
	}
	receipt.RecordedAt = fromUnixNano(recorded)
	return StoreApplyReceiptRecord{Receipt: receipt}, nil
}

func (view *sqliteStoreView) LoadWorkspaceHead(ref WorkspaceRef) (StoreWorkspaceHeadRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreWorkspaceHeadRecord{}, err
	}
	var head WorkspaceHead
	var updated int64
	head.Workspace = ref
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT confirmed_revision, updated_at
		FROM agent_workspace_heads
		WHERE authority_id = ? AND workspace_id = ?`, ref.Authority, ref.ID,
	).Scan(&head.ConfirmedRevision, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreWorkspaceHeadRecord{}, sqliteNotFound(StoreRecordWorkspaceHead, ref.ID)
	}
	if err != nil {
		return StoreWorkspaceHeadRecord{}, fmt.Errorf("load Agent Store Workspace head: %w", err)
	}
	head.UpdatedAt = fromUnixNano(updated)
	return StoreWorkspaceHeadRecord{Head: head}, nil
}

func (view *sqliteStoreView) LoadLeaseGrant(
	ref TaskRef,
	fence LeaseFence,
) (StoreLeaseGrantRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreLeaseGrantRecord{}, err
	}
	var record StoreLeaseGrantRecord
	var granted int64
	record.Grant.Task = ref
	record.Grant.Fence = fence
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT worker_id, granted_at
		FROM agent_lease_grants
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND fence = ?`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID, fence,
	).Scan(&record.Grant.Worker, &granted)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreLeaseGrantRecord{}, sqliteNotFound(StoreRecordLeaseGrant, fence)
	}
	if err != nil {
		return StoreLeaseGrantRecord{}, fmt.Errorf("load Agent Store lease grant: %w", err)
	}
	record.GrantedAt = fromUnixNano(granted)
	return record, nil
}

func (view *sqliteStoreView) LoadLatestLeaseGrant(ref TaskRef) (StoreLeaseGrantRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreLeaseGrantRecord{}, err
	}
	var record StoreLeaseGrantRecord
	var granted int64
	record.Grant.Task = ref
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT fence, worker_id, granted_at
		FROM agent_lease_grants
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ?
		ORDER BY fence DESC LIMIT 1`,
		ref.Workspace.Authority, ref.Workspace.ID, ref.ID,
	).Scan(&record.Grant.Fence, &record.Grant.Worker, &granted)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreLeaseGrantRecord{}, sqliteNotFound(StoreRecordLeaseGrant, ref.ID)
	}
	if err != nil {
		return StoreLeaseGrantRecord{}, fmt.Errorf("load latest Agent Store lease grant: %w", err)
	}
	record.GrantedAt = fromUnixNano(granted)
	return record, nil
}

func (view *sqliteStoreView) FindClaimableTask(authority AuthorityID) (StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreTaskRecord{}, err
	}
	var ref TaskRef
	ref.Workspace.Authority = authority
	err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT workspace_id, task_id
		FROM agent_tasks
		WHERE authority_id = ? AND lease_owner = ''
		  AND state NOT IN ('completed', 'canceled', 'rejected', 'failed')
		ORDER BY created_at, workspace_id, task_id
		LIMIT 1`, authority,
	).Scan(&ref.Workspace.ID, &ref.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return StoreTaskRecord{}, sqliteNotFound(StoreRecordTask, "claimable")
	}
	if err != nil {
		return StoreTaskRecord{}, fmt.Errorf("find claimable Agent Store task: %w", err)
	}
	return view.LoadTask(ref)
}

func (view *sqliteStoreView) ScanContextTasks(
	scan StoreTaskContextScan,
) ([]StoreTaskRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := validateSQLiteScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	query := `
		SELECT workspace_id, task_id
		FROM agent_tasks
		WHERE authority_id = ? AND context_id = ?`
	arguments := []any{scan.Context.Authority, scan.Context.ID}
	if scan.After != nil {
		created := unixNano(scan.After.CreatedAt.UTC())
		query += ` AND (created_at > ? OR
		  (created_at = ? AND (workspace_id > ? OR
		    (workspace_id = ? AND task_id > ?))))`
		arguments = append(
			arguments,
			created, created, scan.After.Workspace, scan.After.Workspace, scan.After.Task,
		)
	}
	query += ` ORDER BY created_at, workspace_id, task_id LIMIT ?`
	arguments = append(arguments, scan.Limit)
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("scan Agent Store context tasks: %w", err)
	}
	refs := make([]TaskRef, 0, scan.Limit)
	for rows.Next() {
		ref := TaskRef{Workspace: WorkspaceRef{Authority: scan.Context.Authority}}
		if err := rows.Scan(&ref.Workspace.ID, &ref.ID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan Agent Store context task key: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("scan Agent Store context tasks: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close Agent Store context task scan: %w", err)
	}
	records := make([]StoreTaskRecord, 0, len(refs))
	for _, ref := range refs {
		record, err := view.LoadTask(ref)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return cloneStoreTaskRecords(records), nil
}

func (view *sqliteStoreView) ScanAuthorityTasks(
	scan StoreTaskAuthorityScan,
) (StoreTaskAuthorityResult, error) {
	if err := view.unit.ensureActive(); err != nil {
		return StoreTaskAuthorityResult{}, err
	}
	if err := validateSQLiteScanLimit(scan.Limit); err != nil {
		return StoreTaskAuthorityResult{}, err
	}
	filters := []string{"authority_id = ?"}
	filterArguments := []any{scan.Authority}
	if scan.Context != "" {
		filters = append(filters, "context_id = ?")
		filterArguments = append(filterArguments, scan.Context)
	}
	if scan.State != "" {
		filters = append(filters, "state = ?")
		filterArguments = append(filterArguments, scan.State)
	}
	if scan.UpdatedAtOrAfter != nil {
		filters = append(filters, "updated_at >= ?")
		filterArguments = append(filterArguments, unixNano(scan.UpdatedAtOrAfter.UTC()))
	}
	where := strings.Join(filters, " AND ")
	var total int64
	if err := view.unit.tx.QueryRowContext(
		view.unit.ctx,
		"SELECT COUNT(*) FROM agent_tasks WHERE "+where,
		filterArguments...,
	).Scan(&total); err != nil {
		return StoreTaskAuthorityResult{}, fmt.Errorf("count Agent Store authority tasks: %w", err)
	}
	if total < 0 {
		return StoreTaskAuthorityResult{}, fmt.Errorf("%w: negative Agent Store task count", ErrCorruptStore)
	}
	pageWhere := where
	pageArguments := append([]any(nil), filterArguments...)
	if scan.After != nil {
		updated := unixNano(scan.After.UpdatedAt.UTC())
		pageWhere += ` AND (updated_at < ? OR
		  (updated_at = ? AND (workspace_id > ? OR
		    (workspace_id = ? AND task_id > ?))))`
		pageArguments = append(
			pageArguments,
			updated, updated, scan.After.Workspace, scan.After.Workspace, scan.After.Task,
		)
	}
	pageArguments = append(pageArguments, scan.Limit)
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, `
		SELECT workspace_id, task_id
		FROM agent_tasks
		WHERE `+pageWhere+`
		ORDER BY updated_at DESC, workspace_id, task_id
		LIMIT ?`, pageArguments...)
	if err != nil {
		return StoreTaskAuthorityResult{}, fmt.Errorf("scan Agent Store authority tasks: %w", err)
	}
	refs := make([]TaskRef, 0, scan.Limit)
	for rows.Next() {
		ref := TaskRef{Workspace: WorkspaceRef{Authority: scan.Authority}}
		if err := rows.Scan(&ref.Workspace.ID, &ref.ID); err != nil {
			_ = rows.Close()
			return StoreTaskAuthorityResult{}, fmt.Errorf("scan Agent Store authority task key: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return StoreTaskAuthorityResult{}, fmt.Errorf("scan Agent Store authority tasks: %w", err)
	}
	if err := rows.Close(); err != nil {
		return StoreTaskAuthorityResult{}, fmt.Errorf("close Agent Store authority task scan: %w", err)
	}
	result := StoreTaskAuthorityResult{
		Records:   make([]StoreTaskRecord, 0, len(refs)),
		TotalSize: uint64(total),
	}
	for _, ref := range refs {
		record, err := view.LoadTask(ref)
		if err != nil {
			return StoreTaskAuthorityResult{}, err
		}
		result.Records = append(result.Records, record)
	}
	result.Records = cloneStoreTaskRecords(result.Records)
	return result, nil
}

func (view *sqliteStoreView) ScanMessages(
	scan StoreMessageScan,
) ([]StoreMessageRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := validateSQLiteScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	if err := validateSQLiteReadLimit(StoreRecordMessage, scan.ReadLimit); err != nil {
		return nil, err
	}
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, `
		SELECT message_id, sequence, author,
		       CASE WHEN length(parts) <= ? THEN parts ELSE NULL END,
		       length(parts), digest, created_at
		FROM agent_messages
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND sequence > ?
		ORDER BY sequence LIMIT ?`,
		scan.ReadLimit.MaxBytes,
		scan.Task.Workspace.Authority, scan.Task.Workspace.ID, scan.Task.ID,
		scan.After, scan.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("scan Agent Store messages: %w", err)
	}
	records := make([]StoreMessageRecord, 0, scan.Limit)
	var total int64
	for rows.Next() {
		var record StoreMessageRecord
		var size int64
		var created int64
		record.Task = scan.Task
		if err := rows.Scan(
			&record.ID, &record.Sequence, &record.Author, &record.EncodedParts,
			&size, &record.PartsDigest, &created,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan Agent Store message: %w", err)
		}
		if size > scan.ReadLimit.MaxBytes || record.EncodedParts == nil {
			_ = rows.Close()
			return nil, &StoreLimitError{
				Record: StoreRecordMessage,
				Limit:  scan.ReadLimit.MaxBytes,
			}
		}
		// An individual record that cannot fit is corruption at the core boundary.
		// Once at least one record fits, however, the aggregate budget is a page
		// boundary: return the contiguous prefix and let Task.MessageCount tell the
		// core that another page exists.
		if total > scan.ReadLimit.MaxBytes-size {
			break
		}
		total += size
		record.EncodedParts = bytes.Clone(record.EncodedParts)
		record.CreatedAt = fromUnixNano(created)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("scan Agent Store messages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close Agent Store message scan: %w", err)
	}
	return cloneStoreMessageRecords(records), nil
}

func (view *sqliteStoreView) ScanEvents(scan StoreEventScan) ([]StoreEventRecord, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := validateSQLiteScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, `
		SELECT sequence, kind, state, revision, message_id, submission_id,
		       artifact_id, created_at
		FROM agent_events
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND sequence > ?
		ORDER BY sequence LIMIT ?`,
		scan.Task.Workspace.Authority, scan.Task.Workspace.ID, scan.Task.ID,
		scan.After, scan.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("scan Agent Store events: %w", err)
	}
	records := make([]StoreEventRecord, 0, scan.Limit)
	for rows.Next() {
		var record StoreEventRecord
		var occurred int64
		record.Event.Task = scan.Task
		if err := rows.Scan(
			&record.Event.Sequence, &record.Event.Type, &record.Event.State,
			&record.Event.Revision, &record.Event.Message, &record.Event.Submission,
			&record.Event.Artifact, &occurred,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan Agent Store event: %w", err)
		}
		record.Event.OccurredAt = fromUnixNano(occurred)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("scan Agent Store events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close Agent Store event scan: %w", err)
	}
	return append([]StoreEventRecord(nil), records...), nil
}

func (view *sqliteStoreView) ScanLeases(scan StoreLeaseScan) ([]LeaseAssignment, error) {
	if err := view.unit.ensureActive(); err != nil {
		return nil, err
	}
	if err := validateSQLiteScanLimit(scan.Limit); err != nil {
		return nil, err
	}
	// Validate current lease ownership before applying the page cursor. Otherwise a
	// missing history row has a NULL granted_at and SQL cursor comparisons would
	// silently filter the corrupt current lease out of every later page.
	var corrupt bool
	if err := view.unit.tx.QueryRowContext(view.unit.ctx, `
		SELECT EXISTS (
		  SELECT 1
		  FROM agent_tasks AS t
		  LEFT JOIN agent_lease_grants AS g
		    ON g.authority_id = t.authority_id
		   AND g.workspace_id = t.workspace_id
		   AND g.task_id = t.task_id
		   AND g.fence = t.lease_fence
		  WHERE t.authority_id = ? AND t.lease_owner = ?
		    AND (g.worker_id IS NULL OR g.worker_id <> t.lease_owner OR g.granted_at IS NULL)
		)`, scan.Authority, scan.Worker,
	).Scan(&corrupt); err != nil {
		return nil, fmt.Errorf("validate Agent Store lease history: %w", err)
	}
	if corrupt {
		return nil, fmt.Errorf("%w: current Agent lease differs from durable grant", ErrCorruptStore)
	}
	query := `
		SELECT t.workspace_id, t.task_id, t.lease_fence, g.worker_id, g.granted_at
		FROM agent_tasks AS t
		LEFT JOIN agent_lease_grants AS g
		  ON g.authority_id = t.authority_id
		 AND g.workspace_id = t.workspace_id
		 AND g.task_id = t.task_id
		 AND g.fence = t.lease_fence
		WHERE t.authority_id = ? AND t.lease_owner = ?`
	arguments := []any{scan.Authority, scan.Worker}
	if scan.After != nil {
		granted := unixNano(scan.After.GrantedAt.UTC())
		query += ` AND (g.granted_at > ? OR
		  (g.granted_at = ? AND (t.workspace_id > ? OR
		    (t.workspace_id = ? AND (t.task_id > ? OR
		      (t.task_id = ? AND t.lease_fence > ?))))))`
		arguments = append(
			arguments,
			granted, granted,
			scan.After.Workspace, scan.After.Workspace,
			scan.After.Task, scan.After.Task, scan.After.Fence,
		)
	}
	query += ` ORDER BY g.granted_at, t.workspace_id, t.task_id, t.lease_fence LIMIT ?`
	arguments = append(arguments, scan.Limit)
	rows, err := view.unit.tx.QueryContext(view.unit.ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("scan Agent Store leases: %w", err)
	}
	type leaseRow struct {
		ref       TaskRef
		fence     LeaseFence
		grantedAt time.Time
	}
	listed := make([]leaseRow, 0, scan.Limit)
	for rows.Next() {
		var row leaseRow
		var historyWorker sql.NullString
		var granted sql.NullInt64
		row.ref.Workspace.Authority = scan.Authority
		if err := rows.Scan(
			&row.ref.Workspace.ID, &row.ref.ID, &row.fence, &historyWorker, &granted,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan Agent Store lease: %w", err)
		}
		if !historyWorker.Valid || WorkerID(historyWorker.String) != scan.Worker || !granted.Valid {
			_ = rows.Close()
			return nil, fmt.Errorf(
				"%w: current Agent lease differs from durable grant", ErrCorruptStore,
			)
		}
		row.grantedAt = fromUnixNano(granted.Int64)
		listed = append(listed, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("scan Agent Store leases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close Agent Store lease scan: %w", err)
	}
	assignments := make([]LeaseAssignment, 0, len(listed))
	for _, row := range listed {
		task, err := view.LoadTask(row.ref)
		if err != nil {
			return nil, err
		}
		if task.Lease.Owner != scan.Worker || task.Lease.Fence != row.fence {
			return nil, fmt.Errorf("%w: scanned lease differs from current Task", ErrCorruptStore)
		}
		assignments = append(assignments, LeaseAssignment{
			Grant: LeaseGrant{Task: row.ref, Worker: scan.Worker, Fence: row.fence},
			Task:  task.Task, GrantedAt: row.grantedAt,
		})
	}
	cloned := make([]LeaseAssignment, len(assignments))
	for index := range assignments {
		cloned[index] = cloneLeaseAssignment(assignments[index])
	}
	return cloned, nil
}

func (unit *sqliteStoreTx) InsertCommand(record StoreCommandRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	result := bytes.Clone(record.Result)
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_commands (
		  authority_id, command_id, kind, digest, result_kind,
		  result, result_digest, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Authority, record.ID, record.Kind, record.IntentDigest,
		record.ResultKind, result, string(record.ResultDigest), unixNano(record.CreatedAt.UTC()),
	)
	if err != nil {
		if uniqueConstraint(err) {
			return sqliteConflict(StoreConstraintCommandID, record.ID)
		}
		return fmt.Errorf("insert Agent Store command: %w", err)
	}
	return nil
}

func (unit *sqliteStoreTx) InsertTask(record StoreTaskRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	record = cloneStoreTaskRecord(record)
	if record.Task.Artifact != nil || record.Task.Submission != nil || record.ArtifactState != nil {
		return fmt.Errorf("%w: inserted Task cannot pre-bind Artifact or submission", ErrInvalidArgument)
	}
	if record.Lease != (StoreLeaseState{}) {
		return fmt.Errorf("%w: inserted Task must begin unleased at fence zero", ErrInvalidArgument)
	}
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_tasks (
		  authority_id, workspace_id, task_id, context_id, state,
		  revision, message_count, event_count, lease_owner, lease_fence,
		  created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Task.Ref.Workspace.Authority, record.Task.Ref.Workspace.ID, record.Task.Ref.ID,
		record.Task.Context.ID, record.Task.State, record.Task.Revision,
		record.Task.MessageCount, record.Task.EventCount, record.Lease.Owner,
		record.Lease.Fence, unixNano(record.Task.CreatedAt.UTC()),
		unixNano(record.Task.UpdatedAt.UTC()),
	)
	if err != nil {
		if uniqueConstraint(err) {
			var count int
			lookupErr := unit.unit.tx.QueryRowContext(unit.unit.ctx, `
				SELECT COUNT(*) FROM agent_tasks
				WHERE authority_id = ? AND workspace_id = ? AND task_id = ?`,
				record.Task.Ref.Workspace.Authority,
				record.Task.Ref.Workspace.ID,
				record.Task.Ref.ID,
			).Scan(&count)
			if lookupErr == nil && count != 0 {
				return sqliteConflict(StoreConstraintTaskKey, record.Task.Ref.ID)
			}
			return sqliteConflict(StoreConstraintPublicTaskID, record.Task.Ref.ID)
		}
		return fmt.Errorf("insert Agent Store task: %w", err)
	}
	return nil
}

func (unit *sqliteStoreTx) CompareAndSwapTask(
	mutation StoreTaskMutation,
) (bool, error) {
	if err := unit.unit.ensureActive(); err != nil {
		return false, err
	}
	mutation.Next = cloneStoreTaskRecord(mutation.Next)
	if mutation.Ref != mutation.Next.Task.Ref || mutation.Condition.ExpectedRevision == 0 {
		return false, fmt.Errorf("%w: invalid Agent Store Task mutation identity", ErrInvalidArgument)
	}
	current, err := unit.LoadTask(mutation.Ref)
	if err != nil {
		return false, err
	}
	if current.Task.Revision != mutation.Condition.ExpectedRevision {
		return false, nil
	}
	if mutation.Condition.ExpectedLease != nil && current.Lease != *mutation.Condition.ExpectedLease {
		return false, nil
	}
	if current.Task.Context != mutation.Next.Task.Context ||
		!current.Task.CreatedAt.Equal(mutation.Next.Task.CreatedAt) {
		return false, fmt.Errorf("%w: Task mutation changes immutable metadata", ErrInvalidArgument)
	}
	query := `
		UPDATE agent_tasks
		SET state = ?, revision = ?, message_count = ?, event_count = ?,
		    lease_owner = ?, lease_fence = ?, updated_at = ?
		WHERE authority_id = ? AND workspace_id = ? AND task_id = ? AND revision = ?`
	arguments := []any{
		mutation.Next.Task.State, mutation.Next.Task.Revision,
		mutation.Next.Task.MessageCount, mutation.Next.Task.EventCount,
		mutation.Next.Lease.Owner, mutation.Next.Lease.Fence,
		unixNano(mutation.Next.Task.UpdatedAt.UTC()),
		mutation.Ref.Workspace.Authority, mutation.Ref.Workspace.ID, mutation.Ref.ID,
		mutation.Condition.ExpectedRevision,
	}
	if mutation.Condition.ExpectedLease != nil {
		query += ` AND lease_owner = ? AND lease_fence = ?`
		arguments = append(
			arguments,
			mutation.Condition.ExpectedLease.Owner,
			mutation.Condition.ExpectedLease.Fence,
		)
	}
	result, err := unit.unit.tx.ExecContext(unit.unit.ctx, query, arguments...)
	if err != nil {
		return false, fmt.Errorf("compare-and-swap Agent Store task: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect Agent Store Task CAS: %w", err)
	}
	return affected == 1, nil
}

func (unit *sqliteStoreTx) InsertMessage(record StoreMessageRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	record = cloneStoreMessageRecord(record)
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_messages (
		  authority_id, message_id, workspace_id, task_id,
		  sequence, author, parts, digest, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.Task.Workspace.Authority, record.ID, record.Task.Workspace.ID,
		record.Task.ID, record.Sequence, record.Author, record.EncodedParts,
		record.PartsDigest, unixNano(record.CreatedAt.UTC()),
	)
	if err != nil {
		if uniqueConstraint(err) {
			var count int
			lookupErr := unit.unit.tx.QueryRowContext(unit.unit.ctx, `
				SELECT COUNT(*) FROM agent_messages
				WHERE authority_id = ? AND message_id = ?`,
				record.Task.Workspace.Authority, record.ID,
			).Scan(&count)
			if lookupErr == nil && count != 0 {
				return sqliteConflict(StoreConstraintMessageID, record.ID)
			}
			return sqliteConflict(StoreConstraintMessageSequence, record.Sequence)
		}
		return fmt.Errorf("insert Agent Store message: %w", err)
	}
	return nil
}

func (unit *sqliteStoreTx) InsertEvent(record StoreEventRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	event := record.Event
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_events (
		  authority_id, workspace_id, task_id, sequence, kind, state,
		  revision, message_id, submission_id, artifact_id, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.Task.Workspace.Authority, event.Task.Workspace.ID, event.Task.ID,
		event.Sequence, event.Type, event.State, event.Revision, event.Message,
		event.Submission, event.Artifact, unixNano(event.OccurredAt.UTC()),
	)
	if err != nil {
		if uniqueConstraint(err) {
			return sqliteConflict(StoreConstraintEventSequence, event.Sequence)
		}
		return fmt.Errorf("insert Agent Store event: %w", err)
	}
	return nil
}

func (unit *sqliteStoreTx) InsertLeaseGrant(record StoreLeaseGrantRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_lease_grants (
		  authority_id, workspace_id, task_id, fence, worker_id, granted_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		record.Grant.Task.Workspace.Authority, record.Grant.Task.Workspace.ID,
		record.Grant.Task.ID, record.Grant.Fence, record.Grant.Worker,
		unixNano(record.GrantedAt.UTC()),
	)
	if err != nil {
		if uniqueConstraint(err) {
			return sqliteConflict(StoreConstraintLeaseFence, record.Grant.Fence)
		}
		return fmt.Errorf("insert Agent Store lease grant: %w", err)
	}
	return nil
}

func (unit *sqliteStoreTx) InsertArtifact(record StoreArtifactRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	record = cloneStoreArtifactRecord(record)
	artifact := record.Artifact
	payload := bytes.Clone(record.EncodedPayload)
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_artifacts (
		  authority_id, workspace_id, artifact_id, task_id, state,
		  base_revision, result_revision, artifact_digest, payload_digest,
		  media_type, payload, payload_size, frozen_at, published_at, discarded_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		artifact.Ref.Workspace.Authority, artifact.Ref.Workspace.ID, artifact.Ref.ID,
		artifact.Task.ID, artifact.State, artifact.BaseRevision, artifact.ResultRevision,
		artifact.Digest, artifact.PayloadDigest, artifact.MediaType, payload,
		artifact.PayloadSize, unixNano(artifact.FrozenAt.UTC()),
		nullableUnixNano(artifact.PublishedAt), nullableUnixNano(artifact.DiscardedAt),
	)
	if err != nil {
		if uniqueConstraint(err) {
			var count int
			lookupErr := unit.unit.tx.QueryRowContext(unit.unit.ctx, `
				SELECT COUNT(*) FROM agent_artifacts
				WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?`,
				artifact.Ref.Workspace.Authority, artifact.Ref.Workspace.ID, artifact.Ref.ID,
			).Scan(&count)
			if lookupErr == nil && count != 0 {
				return sqliteConflict(StoreConstraintArtifactID, artifact.Ref.ID)
			}
			return sqliteConflict(StoreConstraintArtifactTask, artifact.Task.ID)
		}
		return fmt.Errorf("insert Agent Store Artifact: %w", err)
	}
	return nil
}

func (unit *sqliteStoreTx) CompareAndSwapArtifact(
	mutation StoreArtifactMutation,
) (bool, error) {
	if err := unit.unit.ensureActive(); err != nil {
		return false, err
	}
	if mutation.Task.Workspace != mutation.Ref.Workspace {
		return false, fmt.Errorf("%w: Artifact mutation Task belongs to another Workspace", ErrInvalidArgument)
	}
	result, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		UPDATE agent_artifacts
		SET state = ?, published_at = ?, discarded_at = ?
		WHERE authority_id = ? AND workspace_id = ? AND artifact_id = ?
		  AND task_id = ? AND state = ?`,
		mutation.NextState, nullableUnixNano(mutation.PublishedAt),
		nullableUnixNano(mutation.DiscardedAt),
		mutation.Ref.Workspace.Authority, mutation.Ref.Workspace.ID, mutation.Ref.ID,
		mutation.Task.ID, mutation.ExpectedState,
	)
	if err != nil {
		return false, fmt.Errorf("compare-and-swap Agent Store Artifact: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect Agent Store Artifact CAS: %w", err)
	}
	return affected == 1, nil
}

func (unit *sqliteStoreTx) InsertSubmission(record StoreSubmissionRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	submission := cloneStoreSubmission(record.Submission)
	var artifactID any
	if submission.Artifact != nil {
		artifactID = submission.Artifact.ID
	}
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_submissions (
		  authority_id, workspace_id, task_id, submission_id,
		  final_message_id, artifact_id, published_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		submission.Task.Workspace.Authority, submission.Task.Workspace.ID,
		submission.Task.ID, submission.ID, submission.FinalMessage,
		artifactID, unixNano(submission.PublishedAt.UTC()),
	)
	if err != nil {
		if uniqueConstraint(err) {
			return sqliteConflict(StoreConstraintSubmissionID, submission.ID)
		}
		return fmt.Errorf("insert Agent Store submission: %w", err)
	}
	return nil
}

func (unit *sqliteStoreTx) InsertWorkspaceHead(
	record StoreWorkspaceHeadRecord,
) (bool, error) {
	if err := unit.unit.ensureActive(); err != nil {
		return false, err
	}
	head := record.Head
	result, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_workspace_heads (
		  authority_id, workspace_id, confirmed_revision, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(authority_id, workspace_id) DO NOTHING`,
		head.Workspace.Authority, head.Workspace.ID, head.ConfirmedRevision,
		unixNano(head.UpdatedAt.UTC()), unixNano(head.UpdatedAt.UTC()),
	)
	if err != nil {
		return false, fmt.Errorf("insert Agent Store Workspace head: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect Agent Store Workspace head insert: %w", err)
	}
	return affected == 1, nil
}

func (unit *sqliteStoreTx) CompareAndSwapWorkspaceHead(
	mutation StoreWorkspaceHeadMutation,
) (bool, error) {
	if err := unit.unit.ensureActive(); err != nil {
		return false, err
	}
	if mutation.Next.Head.Workspace != mutation.Workspace {
		return false, fmt.Errorf("%w: Workspace head mutation identity mismatch", ErrInvalidArgument)
	}
	result, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		UPDATE agent_workspace_heads
		SET confirmed_revision = ?, updated_at = ?
		WHERE authority_id = ? AND workspace_id = ? AND confirmed_revision = ?`,
		mutation.Next.Head.ConfirmedRevision,
		unixNano(mutation.Next.Head.UpdatedAt.UTC()),
		mutation.Workspace.Authority, mutation.Workspace.ID, mutation.ExpectedRevision,
	)
	if err != nil {
		return false, fmt.Errorf("compare-and-swap Agent Store Workspace head: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect Agent Store Workspace head CAS: %w", err)
	}
	return affected == 1, nil
}

func (unit *sqliteStoreTx) InsertApplyReceipt(record StoreApplyReceiptRecord) error {
	if err := unit.unit.ensureActive(); err != nil {
		return err
	}
	receipt := record.Receipt
	_, err := unit.unit.tx.ExecContext(unit.unit.ctx, `
		INSERT INTO agent_apply_receipts (
		  authority_id, workspace_id, artifact_id, receipt_id, decision,
		  artifact_digest, base_revision, result_revision, observed_revision,
		  code, message, recorded_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		receipt.Artifact.Workspace.Authority, receipt.Artifact.Workspace.ID,
		receipt.Artifact.ID, receipt.ID, receipt.Decision, receipt.ArtifactDigest,
		receipt.BaseRevision, receipt.ResultRevision, receipt.ObservedRevision,
		receipt.Code, receipt.Message, unixNano(receipt.RecordedAt.UTC()),
	)
	if err != nil {
		if uniqueConstraint(err) {
			return sqliteConflict(StoreConstraintReceiptID, receipt.ID)
		}
		return fmt.Errorf("insert Agent Store apply receipt: %w", err)
	}
	return nil
}

func nullableUnixNano(value *time.Time) any {
	if value == nil {
		return nil
	}
	return unixNano(value.UTC())
}

func cloneStoreCommandRecord(record StoreCommandRecord) StoreCommandRecord {
	record.Result = bytes.Clone(record.Result)
	return record
}

func cloneStoreTaskRecord(record StoreTaskRecord) StoreTaskRecord {
	record.Task = cloneTask(record.Task)
	if record.ArtifactState != nil {
		state := *record.ArtifactState
		record.ArtifactState = &state
	}
	return record
}

func cloneStoreTaskRecords(records []StoreTaskRecord) []StoreTaskRecord {
	cloned := make([]StoreTaskRecord, len(records))
	for index := range records {
		cloned[index] = cloneStoreTaskRecord(records[index])
	}
	return cloned
}

func cloneStoreMessageRecord(record StoreMessageRecord) StoreMessageRecord {
	record.EncodedParts = bytes.Clone(record.EncodedParts)
	return record
}

func cloneStoreMessageRecords(records []StoreMessageRecord) []StoreMessageRecord {
	cloned := make([]StoreMessageRecord, len(records))
	for index := range records {
		cloned[index] = cloneStoreMessageRecord(records[index])
	}
	return cloned
}

func cloneStoreArtifactRecord(record StoreArtifactRecord) StoreArtifactRecord {
	record.EncodedPayload = bytes.Clone(record.EncodedPayload)
	if record.Artifact.PublishedAt != nil {
		value := *record.Artifact.PublishedAt
		record.Artifact.PublishedAt = &value
	}
	if record.Artifact.DiscardedAt != nil {
		value := *record.Artifact.DiscardedAt
		record.Artifact.DiscardedAt = &value
	}
	return record
}

func cloneStoreSubmission(submission Submission) Submission {
	if submission.Artifact != nil {
		artifact := *submission.Artifact
		submission.Artifact = &artifact
	}
	return submission
}
