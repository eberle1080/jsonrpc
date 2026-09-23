package collection

import (
	"sync"
	"testing"
	"time"
)

// TestSyncMap_ConcurrentRangeAndWrite verifies that Range() is safe
// to call concurrently with Put/Delete operations
func TestSyncMap_ConcurrentRangeAndWrite(t *testing.T) {
	m := NewSyncMap[string, int]()

	// Pre-populate the map
	for i := 0; i < 100; i++ {
		m.Put(string(rune('a'+i%26)), i)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Start a goroutine that continuously ranges over the map
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				m.Range(func(k string, v int) bool {
					// Just iterate, don't do anything
					_ = k
					_ = v
					return true
				})
			}
		}
	}()

	// Start multiple goroutines that write to the map
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				select {
				case <-done:
					return
				default:
					key := string(rune('a' + (id*j)%26))
					m.Put(key, id*1000+j)
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	// Start multiple goroutines that delete from the map
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				select {
				case <-done:
					return
				default:
					key := string(rune('a' + (id*j)%26))
					m.Delete(key)
					time.Sleep(time.Microsecond * 2)
				}
			}
		}(i)
	}

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)
	close(done)
	wg.Wait()

	// If we got here without a panic, the test passed
}

// TestSyncMap_RangeEarlyExit verifies that Range() stops when callback returns false
func TestSyncMap_RangeEarlyExit(t *testing.T) {
	m := NewSyncMap[int, string]()
	m.Put(1, "one")
	m.Put(2, "two")
	m.Put(3, "three")

	count := 0
	m.Range(func(k int, v string) bool {
		count++
		return count < 2 // Stop after first iteration
	})

	if count != 2 {
		t.Errorf("Expected Range to visit exactly 2 entries, got %d", count)
	}
}

// TestSyncMap_BasicOperations verifies basic Get/Put/Delete functionality
func TestSyncMap_BasicOperations(t *testing.T) {
	m := NewSyncMap[string, int]()

	// Test Put and Get
	m.Put("key1", 42)
	val, ok := m.Get("key1")
	if !ok {
		t.Error("Expected to find key1")
	}
	if val != 42 {
		t.Errorf("Expected value 42, got %d", val)
	}

	// Test Get on non-existent key
	_, ok = m.Get("nonexistent")
	if ok {
		t.Error("Expected not to find nonexistent key")
	}

	// Test Delete
	m.Delete("key1")
	_, ok = m.Get("key1")
	if ok {
		t.Error("Expected key1 to be deleted")
	}

	// Test Delete on non-existent key (should not panic)
	m.Delete("nonexistent")
}

func TestSyncMapRangeAllowsMutationFromCallback(t *testing.T) {
	store := NewSyncMap[string, int]()
	store.Put("a", 1)
	store.Put("b", 2)

	var visited []string
	store.Range(func(key string, value int) bool {
		visited = append(visited, key)
		if key == "a" {
			store.Delete("b")
			store.Put("c", 3)
		}
		return true
	})

	if len(visited) != 2 {
		t.Fatalf("expected snapshot iteration over 2 original entries, got %d (%v)", len(visited), visited)
	}

	if _, ok := store.Get("b"); ok {
		t.Fatalf("expected key b to be deleted during callback")
	}

	if value, ok := store.Get("c"); !ok || value != 3 {
		t.Fatalf("expected key c to be added during callback, got ok=%v value=%d", ok, value)
	}
}
