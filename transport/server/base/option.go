package base

import "github.com/viant/jsonrpc"

// Option represents option
type Option func(s *Session)

func WithFramer(framer FrameMessage) Option {
	return func(s *Session) {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		s.framer = framer
	}
}

// WithEventBuffer sets size of in-memory event buffer for session so that
// server can re-deliver messages on Last-Event-ID reconnect.
func WithEventBuffer(size int) Option {
	return func(s *Session) {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		if size > 0 {
			s.bufferSize = size
		}
	}
}

// WithSSE enables SSE id injection on each framed message and stores
// the same id for resumability (Last-Event-ID).
func WithSSE() Option {
	return func(s *Session) {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		s.sse = true
	}
}

// OverflowPolicy defines how the event buffer handles overflow.
type OverflowPolicy int

const (
	// OverflowDropOldest drops the oldest events when the buffer is full.
	OverflowDropOldest OverflowPolicy = iota
	// OverflowMark sets an overflow flag when buffer would overflow, while still dropping oldest.
	OverflowMark
)

// WithEventOverflowPolicy sets the overflow policy for event buffering.
func WithEventOverflowPolicy(policy OverflowPolicy) Option {
	return func(s *Session) {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		s.overflowPolicy = policy
	}
}

// WithRequestIDGenerator overrides all server-initiated request IDs for this
// session. Stateless transports use it to make response routing globally
// unambiguous across concurrent requests.
func WithRequestIDGenerator(generator func() jsonrpc.RequestId) Option {
	return func(s *Session) {
		s.requestIDGenerator = generator
	}
}

// WithRoundTripLifecycle observes registration and completion of
// server-initiated requests for transports that route responses externally.
func WithRoundTripLifecycle(registered, completed func(jsonrpc.RequestId, *Session)) Option {
	return func(s *Session) {
		s.roundTripRegistered = registered
		s.roundTripCompleted = completed
	}
}
