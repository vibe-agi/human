package workerkit

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/vibe-agi/human/framework"
)

// MemoryStateStore is the reference in-process StateStore. It is the semantic
// model for the conformance suite and the default for tests and ephemeral
// embeddings; it does not survive process restart.
type MemoryStateStore struct {
	mu            sync.Mutex
	closed        bool
	conversations map[ConversationKey]Conversation
}

// NewMemoryStateStore returns an empty store and its release callback.
func NewMemoryStateStore() (*MemoryStateStore, framework.ReleaseFunc) {
	store := &MemoryStateStore{conversations: make(map[ConversationKey]Conversation)}
	return store, func(context.Context) error {
		store.mu.Lock()
		defer store.mu.Unlock()
		store.closed = true
		return nil
	}
}

var _ StateStore = (*MemoryStateStore)(nil)

func (store *MemoryStateStore) SaveConversation(ctx context.Context, conversation Conversation) error {
	if err := checkStateContext(ctx); err != nil {
		return err
	}
	if err := conversation.Key.validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	store.conversations[conversation.Key] = cloneConversation(conversation)
	return nil
}

func (store *MemoryStateStore) DeleteConversation(ctx context.Context, key ConversationKey) error {
	if err := checkStateContext(ctx); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	delete(store.conversations, key)
	return nil
}

func (store *MemoryStateStore) ListConversations(ctx context.Context) ([]Conversation, error) {
	if err := checkStateContext(ctx); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, ErrClosed
	}
	conversations := make([]Conversation, 0, len(store.conversations))
	for _, conversation := range store.conversations {
		conversations = append(conversations, cloneConversation(conversation))
	}
	sort.Slice(conversations, func(left, right int) bool {
		a, b := conversations[left].Key, conversations[right].Key
		if a.Caller != b.Caller {
			return a.Caller < b.Caller
		}
		return a.TaskID < b.TaskID
	})
	return conversations, nil
}

func checkStateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidCommand)
	}
	return ctx.Err()
}
