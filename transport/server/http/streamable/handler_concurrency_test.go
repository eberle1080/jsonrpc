package streamable

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eberle1080/jsonrpc"
	"github.com/eberle1080/jsonrpc/transport"
)

// delayHandler answers every request with {"method": <method>}, sleeping first for
// methods listed in delays. It lets a test hold one request open while others complete.
type delayHandler struct{ delays map[string]time.Duration }

func (h *delayHandler) Serve(_ context.Context, req *jsonrpc.Request, resp *jsonrpc.Response) {
	time.Sleep(h.delays[req.Method])
	resp.Result, _ = json.Marshal(map[string]string{"method": req.Method})
}

func (h *delayHandler) OnNotification(_ context.Context, _ *jsonrpc.Notification) {}

// TestStreamable_ConcurrentPOSTsEachGetTheirOwnResponse reproduces MCP clients sending
// tools/list, resources/list and prompts/list at once on one session. Every POST must
// receive its own response on its own stream, even when a slower request finishes
// after faster ones have completed.
func TestStreamable_ConcurrentPOSTsEachGetTheirOwnResponse(t *testing.T) {
	h := New(func(context.Context, transport.Transport) transport.Handler {
		return &delayHandler{delays: map[string]time.Duration{"slow": 200 * time.Millisecond}}
	}, WithURI("/mcp-test"))

	mux := http.NewServeMux()
	mux.Handle("/mcp-test", h)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/mcp-test", "application/json", nil)
	if err != nil {
		t.Fatalf("handshake POST failed: %v", err)
	}
	_ = resp.Body.Close()
	sid := resp.Header.Get(defaultSessionHeaderKey)
	if sid == "" {
		t.Fatalf("missing session id header %s", defaultSessionHeaderKey)
	}

	post := func(id int, method string) string {
		body := `{"jsonrpc":"2.0","id":` + strconv.Itoa(id) + `,"method":"` + method + `"}`
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp-test", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, "+sseMime)
		req.Header.Set(defaultSessionHeaderKey, sid)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("%s POST failed: %v", method, err)
			return ""
		}
		defer r.Body.Close()
		raw, _ := io.ReadAll(r.Body)
		return string(raw)
	}

	var wg sync.WaitGroup
	var slowBody, fastBody string
	wg.Add(2)
	go func() { defer wg.Done(); slowBody = post(1, "slow") }()
	time.Sleep(50 * time.Millisecond) // slow is in flight; fast attaches and completes first
	go func() { defer wg.Done(); fastBody = post(2, "fast") }()
	wg.Wait()

	if !strings.Contains(fastBody, `"method":"fast"`) {
		t.Errorf("fast POST did not get its own response; body=%q", fastBody)
	}
	if strings.Contains(fastBody, `"method":"slow"`) {
		t.Errorf("fast POST received the slow request's response; body=%q", fastBody)
	}
	if !strings.Contains(slowBody, `"method":"slow"`) {
		t.Errorf("slow POST did not get its own response (it was lost to another stream); body=%q", slowBody)
	}
}
