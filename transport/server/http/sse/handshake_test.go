package sse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/eberle1080/jsonrpc/transport"
	"github.com/eberle1080/jsonrpc/transport/server/base"
	"github.com/eberle1080/jsonrpc/transport/server/http/common"
)

// announcing reports, at the moment the endpoint event is written, whether the
// session it names can already be found.
type announcing struct {
	http.ResponseWriter
	handler    *Handler
	registered *bool
	fail       bool
}

func (a *announcing) Write(p []byte) (int, error) {
	if text := string(p); strings.HasPrefix(text, "event: endpoint") {
		endpoint := strings.TrimSpace(strings.TrimPrefix(strings.SplitN(text, "\n", 3)[1], "data: "))
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return 0, err
		}
		_, found := a.handler.base.Sessions.Get(context.Background(), parsed.Query().Get("session_id"))
		*a.registered = found
		if a.fail {
			return 0, errors.New("client went away")
		}
	}
	return a.ResponseWriter.Write(p)
}

func (a *announcing) Flush() {}

// A client may POST as soon as it reads the endpoint event, so the session
// must be findable by then.
func TestSessionIsRegisteredBeforeItIsAnnounced(t *testing.T) {
	h := New(func(context.Context, transport.Transport) transport.Handler { return &closeTestHandler{} }, WithCleanupInterval(0))
	registered := false
	writer := &announcing{ResponseWriter: httptest.NewRecorder(), handler: h, registered: &registered}
	session, err := h.initSessionHandshake(context.Background(), httptest.NewRequest(http.MethodGet, "/sse", nil), writer, common.NewFlushWriter(writer))
	if err != nil {
		t.Fatal(err)
	}
	if !registered {
		t.Fatal("the endpoint event named a session that could not be found yet")
	}
	if _, found := h.base.Sessions.Get(context.Background(), session.Id); !found {
		t.Fatal("session not registered")
	}

	// A handshake that cannot be written leaves no session behind.
	failing := &announcing{ResponseWriter: httptest.NewRecorder(), handler: h, registered: &registered, fail: true}
	if _, err := h.initSessionHandshake(context.Background(), httptest.NewRequest(http.MethodGet, "/sse", nil), failing, common.NewFlushWriter(failing)); err == nil {
		t.Fatal("expected the handshake to fail")
	}
	count := 0
	h.base.Sessions.Range(context.Background(), func(string, *base.Session) bool { count++; return true })
	if count != 1 {
		t.Fatalf("sessions=%d, want 1", count)
	}
}
