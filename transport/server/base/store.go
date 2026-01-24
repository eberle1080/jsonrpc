package base

import (
	"context"
	"io"

	"github.com/eberle1080/jsonrpc/internal/collection"
	"github.com/eberle1080/jsonrpc/transport"
)

// SessionStore abstracts session persistence.
// Default implementation is in-memory; custom stores (e.g., Redis) can implement this interface.
type SessionStore interface {
	Get(id string) (*Session, bool)
	Put(id string, s *Session)
	Delete(id string)
	Range(func(id string, s *Session) bool)
	// GetOrCreate retrieves a session or creates one if it doesn't exist.
	// The newHandler factory is called to initialize the Handler for new sessions.
	GetOrCreate(id string, ctx context.Context, writer io.Writer, newHandler transport.NewHandler) *Session
}

// memorySessionStore is an in-memory store backed by SyncMap.
type memorySessionStore struct {
	m *collection.SyncMap[string, *Session]
}

func (s *memorySessionStore) Get(id string) (*Session, bool) { return s.m.Get(id) }
func (s *memorySessionStore) Put(id string, v *Session)      { s.m.Put(id, v) }
func (s *memorySessionStore) Delete(id string)               { s.m.Delete(id) }
func (s *memorySessionStore) Range(f func(string, *Session) bool) {
	s.m.Range(f)
}

func (s *memorySessionStore) GetOrCreate(id string, ctx context.Context, writer io.Writer, newHandler transport.NewHandler) *Session {
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
