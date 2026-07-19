package customstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/vibe-agi/human/framework"
	"github.com/vibe-agi/human/llm"
)

const (
	snapshotFormat        = "human.example.llm-store"
	snapshotVersion       = 1
	maxSnapshotSize int64 = 256 << 20
)

var (
	// ErrSnapshotTooLarge means the next atomic image exceeds the configured
	// Store limit. The transaction has not reached the filesystem commit point.
	ErrSnapshotTooLarge = errors.New("custom Store: snapshot too large")
	// ErrStorePoisoned means an atomic rename may have committed but directory
	// durability could not be proved. The handle refuses every later operation;
	// release and reopen is the only reconciliation path.
	ErrStorePoisoned = errors.New("custom Store: handle poisoned by ambiguous commit")
)

// Open creates an application-owned, durable llm.Store. The example uses a
// checksummed, versioned JSON snapshot and an fsync + atomic-rename commit. It
// is deliberately a small single-owner adapter rather than a production
// database driver: every Update rewrites the complete image, which keeps the
// transaction and crash boundary visible to embedders implementing their own
// Store. The parent directory must already exist and is a trusted private
// directory; this compact example does not defend against a process that can
// replace entries in that directory or ignore advisory locks. Supported Unix
// platforms hold a non-blocking advisory lock for the Resource lifetime; other
// platforms fail construction instead of silently running without
// cross-process fencing.
func Open(ctx context.Context, config Config) (framework.Resource[llm.Store], error) {
	if ctx == nil {
		return framework.Resource[llm.Store]{}, errors.New("custom Store: context is required")
	}
	if err := ctx.Err(); err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	if config.Path == "" {
		return framework.Resource[llm.Store]{}, errors.New("custom Store: snapshot path is required")
	}
	description := llm.StoreDescription{
		Contract: framework.Contract{ID: llm.StoreContractID, Major: llm.StoreContractMajor},
		Provider: config.Provider,
		Version:  config.Version,
	}
	if err := description.Validate(); err != nil {
		return framework.Resource[llm.Store]{}, fmt.Errorf("custom Store: description: %w", err)
	}
	limit, err := snapshotLimit(config.MaxSnapshotBytes)
	if err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	path, err := canonicalSnapshotPath(config.Path)
	if err != nil {
		return framework.Resource[llm.Store]{}, fmt.Errorf("custom Store: resolve snapshot path: %w", err)
	}
	pathLock, err := acquirePath(path)
	if err != nil {
		return framework.Resource[llm.Store]{}, err
	}
	bound, state, err := loadSnapshot(ctx, path, limit)
	if err != nil {
		_ = releasePath(path, pathLock)
		return framework.Resource[llm.Store]{}, err
	}
	opened := &fileStore{
		path: path, pathLock: pathLock, maxSnapshotBytes: limit,
		binding: bound, state: state, description: description, audit: config.Audit,
		closeDone: make(chan struct{}), syncDirectory: syncSnapshotDirectory,
	}
	resource, err := framework.Own[llm.Store](opened, opened.release)
	if err != nil {
		opened.finishClose()
		return framework.Resource[llm.Store]{}, err
	}
	return resource, nil
}

type fileStore struct {
	mu               sync.RWMutex
	path             string
	pathLock         *os.File
	maxSnapshotBytes int64
	binding          *llm.StoreBinding
	state            customState
	description      llm.StoreDescription
	audit            AuditFunc
	closed           bool
	poisoned         error
	closeOnce        sync.Once
	closeDone        chan struct{}
	closeErr         error
	syncDirectory    func(string) error
}

var _ llm.Store = (*fileStore)(nil)

func (store *fileStore) Description() llm.StoreDescription {
	if store == nil {
		return llm.StoreDescription{}
	}
	description := store.description
	description.Contract.Features = maps.Clone(description.Contract.Features)
	return description
}

func (store *fileStore) Bind(ctx context.Context, binding llm.StoreBinding) error {
	if ctx == nil {
		return llm.ErrStoreInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	if store == nil {
		return llm.ErrStoreClosed
	}
	store.record("bind")
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.operationErrorLocked(); err != nil {
		return err
	}
	if store.binding != nil {
		if *store.binding != binding {
			return &llm.StoreConflictError{
				Constraint: llm.StoreConstraintDeploymentBinding,
				Key:        binding.DeploymentID,
			}
		}
		return nil
	}
	stored := binding
	return store.commit(ctx, &stored, store.state)
}

func (store *fileStore) View(ctx context.Context, callback func(llm.StoreView) error) error {
	if ctx == nil || callback == nil {
		return llm.ErrStoreInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil {
		return llm.ErrStoreClosed
	}
	store.record("view")
	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.operationErrorLocked(); err != nil {
		return err
	}
	unit := &customUnit{state: &store.state}
	unit.active.Store(true)
	defer unit.active.Store(false)
	return callback(customView{unit: unit})
}

func (store *fileStore) Update(ctx context.Context, callback func(llm.StoreTx) error) error {
	if ctx == nil || callback == nil {
		return llm.ErrStoreInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil {
		return llm.ErrStoreClosed
	}
	store.record("update")
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.operationErrorLocked(); err != nil {
		return err
	}
	next := store.state.clone()
	unit := &customUnit{state: &next}
	unit.active.Store(true)
	err := invokeUpdate(unit, callback)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.commit(ctx, store.binding, next)
}

func invokeUpdate(unit *customUnit, callback func(llm.StoreTx) error) error {
	defer unit.active.Store(false)
	return callback(customTx{customView{unit: unit}})
}

func (store *fileStore) commit(ctx context.Context, binding *llm.StoreBinding, state customState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := encodeSnapshot(binding, state)
	if err != nil {
		return fmt.Errorf("custom Store: encode snapshot: %w", err)
	}
	if int64(len(encoded)) > store.maxSnapshotBytes {
		return fmt.Errorf("%w: encoded image is %d bytes, limit is %d",
			ErrSnapshotTooLarge, len(encoded), store.maxSnapshotBytes)
	}
	committed, err := replaceSnapshot(store.path, encoded, store.syncDirectory)
	if err != nil {
		if committed {
			unknown := &llm.StoreCommitUnknownError{Cause: errors.Join(ErrStorePoisoned, err)}
			store.poisoned = unknown
			return unknown
		}
		return fmt.Errorf("custom Store: commit snapshot: %w", err)
	}
	store.binding = cloneBinding(binding)
	store.state = state
	return nil
}

func (store *fileStore) operationErrorLocked() error {
	if store.closed {
		return llm.ErrStoreClosed
	}
	if store.poisoned != nil {
		return store.poisoned
	}
	return nil
}

func (store *fileStore) release(ctx context.Context) error {
	store.closeOnce.Do(func() { go store.finishClose() })
	if ctx == nil {
		return errors.New("custom Store: release context is required")
	}
	select {
	case <-store.closeDone:
		return store.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (store *fileStore) finishClose() {
	store.mu.Lock()
	store.closed = true
	store.mu.Unlock()
	store.closeErr = releasePath(store.path, store.pathLock)
	close(store.closeDone)
}

func (store *fileStore) record(operation string) {
	if store.audit != nil {
		store.audit(operation)
	}
}

type diskPayload struct {
	Binding  *llm.StoreBinding              `json:"binding,omitempty"`
	Tasks    []llm.StoreTaskRecord          `json:"tasks,omitempty"`
	Requests []llm.StoreRequestRecord       `json:"requests,omitempty"`
	Events   []llm.StoreResponseEventRecord `json:"events,omitempty"`
	Receipts []llm.StoreWorkerReceiptRecord `json:"receipts,omitempty"`
	Tools    []llm.StoreToolExecutionRecord `json:"tools,omitempty"`
}

type diskEnvelope struct {
	Format  string          `json:"format"`
	Version int             `json:"version"`
	SHA256  string          `json:"sha256"`
	Payload json.RawMessage `json:"payload"`
}

func encodeSnapshot(binding *llm.StoreBinding, state customState) ([]byte, error) {
	payload := diskPayload{Binding: cloneBinding(binding)}
	for _, record := range state.tasks {
		payload.Tasks = append(payload.Tasks, cloneCustomTask(record))
	}
	for _, record := range state.requests {
		payload.Requests = append(payload.Requests, cloneCustomRequest(record))
	}
	for _, records := range state.events {
		for _, record := range records {
			payload.Events = append(payload.Events, cloneCustomEvent(record))
		}
	}
	for _, record := range state.receipts {
		payload.Receipts = append(payload.Receipts, record)
	}
	for _, record := range state.tools {
		payload.Tools = append(payload.Tools, cloneCustomTool(record))
	}
	sortDiskPayload(&payload)
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encodedPayload)
	return json.Marshal(diskEnvelope{
		Format: snapshotFormat, Version: snapshotVersion,
		SHA256: hex.EncodeToString(digest[:]), Payload: encodedPayload,
	})
}

func loadSnapshot(ctx context.Context, path string, limit int64) (*llm.StoreBinding, customState, error) {
	state := newCustomState()
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, state, nil
	}
	if err != nil {
		return nil, state, fmt.Errorf("custom Store: open snapshot: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, state, fmt.Errorf("custom Store: inspect snapshot: %w", err)
	}
	if info.Size() < 1 {
		return nil, state, corruptSnapshot(errors.New("snapshot is empty"))
	}
	if info.Size() > limit {
		return nil, state, fmt.Errorf("%w: encoded image is %d bytes, limit is %d",
			ErrSnapshotTooLarge, info.Size(), limit)
	}
	encoded, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, state, fmt.Errorf("custom Store: read snapshot: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, state, err
	}
	var envelope diskEnvelope
	if err := decodeStrict(encoded, &envelope); err != nil {
		return nil, state, corruptSnapshot(err)
	}
	if envelope.Format != snapshotFormat || envelope.Version != snapshotVersion {
		return nil, state, corruptSnapshot(fmt.Errorf("unsupported format %q version %d", envelope.Format, envelope.Version))
	}
	digest := sha256.Sum256(envelope.Payload)
	want, err := hex.DecodeString(envelope.SHA256)
	if err != nil || len(want) != sha256.Size || !bytes.Equal(want, digest[:]) {
		return nil, state, corruptSnapshot(errors.New("snapshot checksum mismatch"))
	}
	var payload diskPayload
	if err := decodeStrict(envelope.Payload, &payload); err != nil {
		return nil, state, corruptSnapshot(err)
	}
	loaded, err := payloadState(payload)
	if err != nil {
		return nil, state, corruptSnapshot(err)
	}
	return cloneBinding(payload.Binding), loaded, nil
}

func payloadState(payload diskPayload) (customState, error) {
	state := newCustomState()
	if payload.Binding != nil {
		if err := payload.Binding.Validate(); err != nil {
			return state, err
		}
	}
	unit := &customUnit{state: &state}
	unit.active.Store(true)
	defer unit.active.Store(false)
	tx := customTx{customView{unit: unit}}
	for _, record := range payload.Tasks {
		if err := tx.InsertTask(record); err != nil {
			return state, fmt.Errorf("load task %v: %w", record.Key, err)
		}
	}
	for _, record := range payload.Requests {
		if err := tx.InsertRequest(record); err != nil {
			return state, fmt.Errorf("load request %v: %w", record.Key, err)
		}
	}
	for _, record := range payload.Events {
		if err := tx.InsertResponseEvent(record); err != nil {
			return state, fmt.Errorf("load event %v/%d: %w", record.Request, record.Sequence, err)
		}
	}
	for _, record := range payload.Receipts {
		if err := tx.InsertWorkerReceipt(record); err != nil {
			return state, fmt.Errorf("load receipt %v/%s: %w", record.Request, record.EventID, err)
		}
	}
	for _, record := range payload.Tools {
		if err := tx.InsertToolExecution(record); err != nil {
			return state, fmt.Errorf("load tool execution %v: %w", record.Key, err)
		}
	}
	return state, validateLoadedRelationships(state)
}

func validateLoadedRelationships(state customState) error {
	for request, events := range state.events {
		head := state.requests[request]
		for sequence := range events {
			if sequence > head.LastEventSequence && head.LastEventSequence != 0 {
				return fmt.Errorf("event sequence %d exceeds request head %d", sequence, head.LastEventSequence)
			}
		}
	}
	return nil
}

func decodeStrict(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing snapshot value")
		}
		return err
	}
	return nil
}

func replaceSnapshot(
	path string,
	encoded []byte,
	syncDirectory func(string) error,
) (committed bool, err error) {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".human-store-*")
	if err != nil {
		return false, err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return false, err
	}
	if _, err := temporary.Write(encoded); err != nil {
		return false, err
	}
	if err := temporary.Sync(); err != nil {
		return false, err
	}
	if err := temporary.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return false, err
	}
	committed = true
	if err := syncDirectory(directory); err != nil {
		return true, err
	}
	return true, nil
}

func sortDiskPayload(payload *diskPayload) {
	sort.Slice(payload.Tasks, func(i, j int) bool {
		if payload.Tasks[i].Key.Caller != payload.Tasks[j].Key.Caller {
			return payload.Tasks[i].Key.Caller < payload.Tasks[j].Key.Caller
		}
		return payload.Tasks[i].Key.Task < payload.Tasks[j].Key.Task
	})
	sort.Slice(payload.Requests, func(i, j int) bool {
		if payload.Requests[i].Key.Caller != payload.Requests[j].Key.Caller {
			return payload.Requests[i].Key.Caller < payload.Requests[j].Key.Caller
		}
		return payload.Requests[i].Key.IdempotencyKey < payload.Requests[j].Key.IdempotencyKey
	})
	sort.Slice(payload.Events, func(i, j int) bool {
		if payload.Events[i].Request.Caller != payload.Events[j].Request.Caller {
			return payload.Events[i].Request.Caller < payload.Events[j].Request.Caller
		}
		if payload.Events[i].Request.IdempotencyKey != payload.Events[j].Request.IdempotencyKey {
			return payload.Events[i].Request.IdempotencyKey < payload.Events[j].Request.IdempotencyKey
		}
		return payload.Events[i].Sequence < payload.Events[j].Sequence
	})
	sort.Slice(payload.Receipts, func(i, j int) bool {
		if payload.Receipts[i].Request.Caller != payload.Receipts[j].Request.Caller {
			return payload.Receipts[i].Request.Caller < payload.Receipts[j].Request.Caller
		}
		if payload.Receipts[i].Request.IdempotencyKey != payload.Receipts[j].Request.IdempotencyKey {
			return payload.Receipts[i].Request.IdempotencyKey < payload.Receipts[j].Request.IdempotencyKey
		}
		return payload.Receipts[i].EventID < payload.Receipts[j].EventID
	})
	sort.Slice(payload.Tools, func(i, j int) bool {
		left, right := payload.Tools[i].Key, payload.Tools[j].Key
		if left.Task.Caller != right.Task.Caller {
			return left.Task.Caller < right.Task.Caller
		}
		if left.Task.Task != right.Task.Task {
			return left.Task.Task < right.Task.Task
		}
		return left.ToolCallID < right.ToolCallID
	})
}

func corruptSnapshot(cause error) error {
	return &llm.StoreCorruptError{Key: "snapshot", Cause: cause}
}

func cloneBinding(binding *llm.StoreBinding) *llm.StoreBinding {
	if binding == nil {
		return nil
	}
	cloned := *binding
	return &cloned
}

var openPaths = struct {
	sync.Mutex
	active map[string]struct{}
}{active: make(map[string]struct{})}

func acquirePath(path string) (*os.File, error) {
	openPaths.Lock()
	defer openPaths.Unlock()
	if _, exists := openPaths.active[path]; exists {
		return nil, fmt.Errorf("custom Store: snapshot %q is already open in this process", path)
	}
	lock, err := acquireSnapshotLock(path + ".lock")
	if err != nil {
		return nil, err
	}
	openPaths.active[path] = struct{}{}
	return lock, nil
}

func releasePath(path string, lock *os.File) error {
	openPaths.Lock()
	err := releaseSnapshotLock(lock)
	delete(openPaths.active, path)
	openPaths.Unlock()
	return err
}

func snapshotLimit(configured int64) (int64, error) {
	if configured < 0 || configured > maxSnapshotSize {
		return 0, fmt.Errorf("custom Store: MaxSnapshotBytes must be 0..%d", maxSnapshotSize)
	}
	if configured == 0 {
		return maxSnapshotSize, nil
	}
	return configured, nil
}

func canonicalSnapshotPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	directory := filepath.Dir(absolute)
	realDirectory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return "", err
	}
	canonical := filepath.Join(realDirectory, filepath.Base(absolute))
	info, err := os.Lstat(canonical)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("snapshot path must not be a symbolic link")
		}
		if !info.Mode().IsRegular() {
			return "", errors.New("snapshot path must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return canonical, nil
}

func syncSnapshotDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
