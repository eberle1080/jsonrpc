package base

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCloseSessionOnce(t *testing.T) {
	store := NewMemorySessionStore()
	session := &Session{Id: "same"}
	store.Put(context.Background(), session.Id, session)
	var calls atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			CloseSession(context.Background(), store, session.Id, func(got *Session) {
				if got != session {
					t.Error("wrong session")
				}
				if _, ok := store.Get(context.Background(), session.Id); ok {
					t.Error("hook before removal")
				}
				calls.Add(1)
			})
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("close calls=%d", calls.Load())
	}
}
