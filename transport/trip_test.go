package transport

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/eberle1080/jsonrpc"
)

func TestRoundTripsRejectsActiveDuplicateAndAllowsAfterMatch(t *testing.T) {
	trips := NewRoundTrips(2)
	request := &jsonrpc.Request{Id: 7, Jsonrpc: jsonrpc.Version, Method: "first"}

	if _, err := trips.Add(request); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if _, err := trips.Add(&jsonrpc.Request{Id: 7, Jsonrpc: jsonrpc.Version, Method: "second"}); err == nil {
		t.Fatalf("Add() duplicate error = nil, want error")
	} else if !strings.Contains(err.Error(), "duplicate active request id: 7") {
		t.Fatalf("Add() duplicate error = %q, want duplicate request id", err)
	}

	if _, err := trips.Match(7); err != nil {
		t.Fatalf("Match() error = %v", err)
	}
	if _, err := trips.Add(&jsonrpc.Request{Id: 7, Jsonrpc: jsonrpc.Version, Method: "again"}); err != nil {
		t.Fatalf("Add() after Match error = %v", err)
	}

	stringTrips := NewRoundTrips(3)
	if _, err := stringTrips.Add(&jsonrpc.Request{Id: "a", Jsonrpc: jsonrpc.Version, Method: "first"}); err != nil {
		t.Fatalf("Add() string id a error = %v", err)
	}
	if _, err := stringTrips.Add(&jsonrpc.Request{Id: "b", Jsonrpc: jsonrpc.Version, Method: "second"}); err != nil {
		t.Fatalf("Add() distinct string id b error = %v", err)
	}
	if _, err := stringTrips.Add(&jsonrpc.Request{Id: "a", Jsonrpc: jsonrpc.Version, Method: "duplicate"}); err == nil {
		t.Fatalf("Add() duplicate string id error = nil, want error")
	}
	if _, err := stringTrips.Match("a"); err != nil {
		t.Fatalf("Match() string id error = %v", err)
	}
	if _, err := stringTrips.Add(&jsonrpc.Request{Id: "a", Jsonrpc: jsonrpc.Version, Method: "again"}); err != nil {
		t.Fatalf("Add() string id after Match error = %v", err)
	}
}

func TestRoundTripsConcurrentAddMatch(t *testing.T) {
	const (
		workers = 16
		rounds  = 100
	)
	trips := NewRoundTrips(workers * rounds)

	var wg sync.WaitGroup
	errs := make(chan error, workers*rounds*2)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				id := worker*rounds + i + 1
				request := &jsonrpc.Request{Id: id, Jsonrpc: jsonrpc.Version, Method: "call"}
				if _, err := trips.Add(request); err != nil {
					errs <- err
					continue
				}
				if _, err := trips.Match(id); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("RoundTrips concurrent Add/Match error = %v", err)
	}

	if _, err := trips.Add(&jsonrpc.Request{Id: 1, Jsonrpc: jsonrpc.Version, Method: "reuse"}); err != nil {
		t.Fatalf("Add() reused id error = %v", err)
	}
	if _, err := trips.Match(1); err != nil {
		t.Fatalf("Match() reused id error = %v", err)
	}
}

func TestRoundTripsConcurrentCloseGetSize(t *testing.T) {
	trips := NewRoundTrips(64)
	for i := 0; i < 32; i++ {
		if _, err := trips.Add(&jsonrpc.Request{Id: i + 1, Jsonrpc: jsonrpc.Version, Method: "call"}); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	closedErr := errors.New("closed")
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				size := trips.Size()
				for index := 0; index < size; index++ {
					_ = trips.Get(index)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			trips.CloseWithError(closedErr)
		}
	}()
	wg.Wait()

	if _, err := trips.Add(&jsonrpc.Request{Id: 100, Jsonrpc: jsonrpc.Version, Method: "after-close"}); !errors.Is(err, closedErr) {
		t.Fatalf("Add() after CloseWithError error = %v, want %v", err, closedErr)
	}
	if _, err := trips.Match(1); !errors.Is(err, closedErr) {
		t.Fatalf("Match() after CloseWithError error = %v, want %v", err, closedErr)
	}
}
