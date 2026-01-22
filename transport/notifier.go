package transport

import (
	"context"
	"github.com/eberle1080/jsonrpc"
)

// Notifier represents a notification handler
type Notifier interface {
	Notify(ctx context.Context, notification *jsonrpc.Notification) error
}
