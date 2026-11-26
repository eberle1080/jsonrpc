package base

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/eberle1080/jsonrpc/internal/collection"
	"github.com/eberle1080/jsonrpc/transport"
)

// SessionStore abstracts session persistence.
// Default implementation is in-memory; custom stores (e.g., Redis) can implement this interface.
type SessionStore interface {
	Get(ctx context.Context, id string) (*Session, bool)
	Put(ctx context.Context, id string, s *Session)
	Delete(ctx context.Context, id string)
	Range(ctx context.Context, f func(id string, s *Session) bool)
	// GetOrCreate retrieves a session or creates one if it doesn't exist.
	// The newHandler factory is called to initialize the Handler for new sessions.
	GetOrCreate(ctx context.Context, id string, writer io.Writer, newHandler transport.NewHandler) *Session
}

// memorySessionStore is an in-memory store backed by SyncMap.
type memorySessionStore struct {
	m *collection.SyncMap[string, *Session]
}

func (s *memorySessionStore) Get(ctx context.Context, id string) (*Session, bool) {
	return s.m.Get(id)
}
func (s *memorySessionStore) Put(ctx context.Context, id string, v *Session) { s.m.Put(id, v) }
func (s *memorySessionStore) Delete(ctx context.Context, id string)          { s.m.Delete(id) }
func (s *memorySessionStore) Range(ctx context.Context, f func(string, *Session) bool) {
	s.m.Range(f)
}

func (s *memorySessionStore) GetOrCreate(ctx context.Context, id string, writer io.Writer, newHandler transport.NewHandler) *Session {
	if existing, ok := s.m.Get(id); ok {
		return existing
	}
	// Create new session with the provided id
	session := NewSession(ctx, id, writer, newHandler)
	s.m.Put(id, session)
	return session
}

// NewMemorySessionStore creates an in-memory SessionStore.
func NewMemorySessionStore() SessionStore {
	return &memorySessionStore{m: collection.NewSyncMap[string, *Session]()}
}

// CloseSession removes a session and invokes its final-close hook at most once.
// The hook runs after removal and outside session/store locks.
func CloseSession(ctx context.Context, store SessionStore, id string, closed func(*Session)) {
	session, ok := store.Get(ctx, id)
	if !ok {
		return
	}
	store.Delete(ctx, id)
	session.Close(closed)
}

// Close invokes the final session-close hook once, including request-scoped
// stateless sessions which are never inserted into a session store.
func (session *Session) Close(closed func(*Session)) {
	if !atomic.CompareAndSwapInt32(&session.closed, 0, 1) {
		return
	}
	if closed != nil {
		defer func() { _ = recover() }()
		closed(session)
	}
}
