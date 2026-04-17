package outbox

import (
	"context"

	"github.com/vectrum-io/strongforce/pkg/events"
)

// CommitNotifier is invoked after a successful EventTx/EventsTx commit.
// Implementations MUST be non-blocking: they run on the commit critical path.
type CommitNotifier interface {
	NotifyCommitted(ctx context.Context, events []*events.SerializedEvent)
}
