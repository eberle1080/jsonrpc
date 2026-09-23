package base

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/eberle1080/jsonrpc"
	"github.com/eberle1080/jsonrpc/transport"
)

type recordingTransport struct {
	mu       sync.Mutex
	requests []*jsonrpc.Request
	sent     chan *jsonrpc.Request
}

func newRecordingTransport() *recordingTransport {
	return &recordingTransport{
		sent: make(chan *jsonrpc.Request, 10),
	}
}

func (t *recordingTransport) SendData(ctx context.Context, data []byte) error {
	request := &jsonrpc.Request{}
	if err := json.Unmarshal(data, request); err != nil {
		return err
	}
	t.mu.Lock()
	t.requests = append(t.requests, request)
	t.mu.Unlock()

	select {
	case t.sent <- request:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestClientSendAdvancesSequenceForExplicitNumericID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	recorder := newRecordingTransport()
	client := &Client{
		Transport:  recorder,
		RoundTrips: transport.NewRoundTrips(10),
		RunTimeout: time.Second,
		Handler:    &Handler{},
	}

	firstResult := sendAsync(ctx, client, &jsonrpc.Request{Jsonrpc: jsonrpc.Version, Method: "initialize"})
	firstSent := waitSentRequest(t, recorder)
	if got, ok := jsonrpc.AsRequestIntId(firstSent.Id); !ok || got != 1 {
		t.Fatalf("first auto request id = %v, want 1", firstSent.Id)
	}
	client.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":1,"result":"initialized"}`))
	assertResponseResult(t, firstResult, `"initialized"`)

	explicitResult := sendAsync(ctx, client, &jsonrpc.Request{Jsonrpc: jsonrpc.Version, Method: "tools/call", Id: 2})
	explicitSent := waitSentRequest(t, recorder)
	if got, ok := jsonrpc.AsRequestIntId(explicitSent.Id); !ok || got != 2 {
		t.Fatalf("explicit request id = %v, want 2", explicitSent.Id)
	}
	assertStillPending(t, explicitResult)

	pingResult := sendAsync(ctx, client, &jsonrpc.Request{Jsonrpc: jsonrpc.Version, Method: "ping"})
	pingSent := waitSentRequest(t, recorder)
	if got, ok := jsonrpc.AsRequestIntId(pingSent.Id); !ok || got != 3 {
		t.Fatalf("second auto request id = %v, want 3", pingSent.Id)
	}

	client.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":3,"result":"pong"}`))
	assertResponseResult(t, pingResult, `"pong"`)

	client.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":2,"result":"tool-result"}`))
	assertResponseResult(t, explicitResult, `"tool-result"`)
}

func TestRoundTripWaitWithoutTransportTimeoutUsesContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	trip := transport.NewRoundTrip(&jsonrpc.Request{Id: 1, Jsonrpc: jsonrpc.Version, Method: "subscriptions/listen"})
	started := time.Now()
	err := trip.Wait(ctx, 0)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline, got %v", err)
	}
	if time.Since(started) < 15*time.Millisecond {
		t.Fatal("wait returned before context cancellation")
	}
}

type sendResult struct {
	response *jsonrpc.Response
	err      error
}

func sendAsync(ctx context.Context, client *Client, request *jsonrpc.Request) <-chan sendResult {
	result := make(chan sendResult, 1)
	go func() {
		response, err := client.Send(ctx, request)
		result <- sendResult{response: response, err: err}
	}()
	return result
}

func waitSentRequest(t *testing.T, recorder *recordingTransport) *jsonrpc.Request {
	t.Helper()
	select {
	case request := <-recorder.sent:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sent request")
	}
	return nil
}

func assertStillPending(t *testing.T, result <-chan sendResult) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatalf("request completed early: response=%v err=%v", got.response, got.err)
	default:
	}
}

func assertResponseResult(t *testing.T, result <-chan sendResult, want string) {
	t.Helper()
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("Send() error = %v", got.err)
		}
		if got.response == nil {
			t.Fatalf("Send() response = nil")
		}
		if string(got.response.Result) != want {
			t.Fatalf("Send() result = %s, want %s", got.response.Result, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for response %s", want)
	}
}
