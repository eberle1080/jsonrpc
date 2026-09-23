package streamable

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eberle1080/jsonrpc"
	"github.com/eberle1080/jsonrpc/transport"
	streamableclient "github.com/eberle1080/jsonrpc/transport/client/http/streamable"
)

type statelessStreamingHandler struct {
	transport transport.Transport
}

func (h *statelessStreamingHandler) Serve(ctx context.Context, request *jsonrpc.Request, response *jsonrpc.Response) {
	_ = h.transport.Notify(ctx, &jsonrpc.Notification{
		Method: "notifications/acknowledged",
		Params: json.RawMessage(`{"requestId":1}`),
	})
	clientResponse, err := h.transport.Send(ctx, &jsonrpc.Request{
		Jsonrpc: jsonrpc.Version,
		Method:  "client/echo",
		Params:  json.RawMessage(`{"value":"hello"}`),
	})
	if err != nil {
		response.Error = jsonrpc.NewInternalError(err.Error(), nil)
		return
	}
	response.Result = clientResponse.Result
}

func (h *statelessStreamingHandler) OnNotification(context.Context, *jsonrpc.Notification) {}

type notificationRecorder struct {
	mu      sync.Mutex
	methods []string
}

func (r *notificationRecorder) Serve(_ context.Context, request *jsonrpc.Request, response *jsonrpc.Response) {
	response.Id = request.Id
	response.Jsonrpc = request.Jsonrpc
	if request.Method == "client/echo" {
		response.Result = json.RawMessage(`{"echoed":true}`)
	}
}

func (r *notificationRecorder) OnNotification(_ context.Context, notification *jsonrpc.Notification) {
	r.mu.Lock()
	r.methods = append(r.methods, notification.Method)
	r.mu.Unlock()
}

func TestStatelessStreamableHTTP_StreamsNotificationAndFinalResponse(t *testing.T) {
	serverHandler := New(func(_ context.Context, transport transport.Transport) transport.Handler {
		return &statelessStreamingHandler{transport: transport}
	}, WithStateless(), WithCleanupInterval(0))
	server := httptest.NewServer(serverHandler)
	defer server.Close()

	recorder := &notificationRecorder{}
	client, err := streamableclient.New(context.Background(), server.URL,
		streamableclient.WithStateless(),
		streamableclient.WithRunTimeout(time.Second),
		streamableclient.WithHandler(recorder))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	response, err := client.Send(context.Background(), &jsonrpc.Request{Jsonrpc: jsonrpc.Version, Method: "subscriptions/listen"})
	if err != nil {
		t.Fatal(err)
	}
	if response == nil || string(response.Result) != `{"echoed":true}` {
		t.Fatalf("unexpected final response: %#v", response)
	}
	if client.SessionID() != "" {
		t.Fatalf("stateless client unexpectedly captured session %q", client.SessionID())
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.methods) != 1 || recorder.methods[0] != "notifications/acknowledged" {
		t.Fatalf("unexpected notifications: %v", recorder.methods)
	}
}

func TestStatelessResolverPreservesLegacyStatefulDefault(t *testing.T) {
	handler := New(func(_ context.Context, transport transport.Transport) transport.Handler {
		return &statelessStreamingHandler{transport: transport}
	}, WithStatelessResolver(func(request *http.Request) bool {
		return request.Header.Get("X-Stateless") == "true"
	}), WithCleanupInterval(0))

	notification := `{"jsonrpc":"2.0","method":"notifications/test"}`
	legacy := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(notification))
	legacyResponse := httptest.NewRecorder()
	handler.ServeHTTP(legacyResponse, legacy)
	if legacyResponse.Header().Get(defaultSessionHeaderKey) == "" {
		t.Fatal("legacy request did not receive a session id")
	}

	stateless := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(notification))
	stateless.Header.Set("X-Stateless", "true")
	statelessResponse := httptest.NewRecorder()
	handler.ServeHTTP(statelessResponse, stateless)
	if statelessResponse.Header().Get(defaultSessionHeaderKey) != "" {
		t.Fatal("stateless request unexpectedly received a session id")
	}
}

func TestStatelessStreamableHTTP_AllowsConcurrentRequests(t *testing.T) {
	serverHandler := New(func(_ context.Context, transport transport.Transport) transport.Handler {
		return &statelessStreamingHandler{transport: transport}
	}, WithStateless(), WithCleanupInterval(0))
	server := httptest.NewServer(serverHandler)
	defer server.Close()

	client, err := streamableclient.New(context.Background(), server.URL,
		streamableclient.WithStateless(),
		streamableclient.WithRunTimeout(2*time.Second),
		streamableclient.WithHandler(&notificationRecorder{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const count = 8
	errors := make(chan error, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response, err := client.Send(context.Background(), &jsonrpc.Request{Jsonrpc: jsonrpc.Version, Method: "ping"})
			if err == nil && (response == nil || string(response.Result) != `{"echoed":true}`) {
				err = fmt.Errorf("unexpected response: %#v", response)
			}
			errors <- err
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}
