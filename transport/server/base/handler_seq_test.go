package base

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/eberle1080/jsonrpc"
	"github.com/eberle1080/jsonrpc/transport"
)

type noopHandler struct{}

func (noopHandler) Serve(context.Context, *jsonrpc.Request, *jsonrpc.Response) {}
func (noopHandler) OnNotification(context.Context, *jsonrpc.Notification)      {}

// TestHandleMessage_RequestIdSeqNeverMovesBackwards sends many concurrent requests and checks
// the session sequence ends at the highest client id; a racy load/store could leave it lower.
func TestHandleMessage_RequestIdSeqNeverMovesBackwards(t *testing.T) {
	for run := 0; run < 20; run++ {
		sess := NewSession(context.Background(), "s", nil, func(context.Context, transport.Transport) transport.Handler {
			return noopHandler{}
		})
		h := &Handler{}

		const n = 200
		var wg sync.WaitGroup
		for i := 1; i <= n; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				msg := []byte(`{"jsonrpc":"2.0","id":` + strconv.Itoa(id) + `,"method":"m"}`)
				var out bytes.Buffer
				h.HandleMessage(context.Background(), sess, msg, &out)
			}(i)
		}
		wg.Wait()

		if got := atomic.LoadUint64(&sess.RequestIdSeq); got < n {
			t.Fatalf("run %d: RequestIdSeq = %d, want >= %d", run, got, n)
		}
	}
}
