package outbox

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

type capturingNotifier struct {
	calls atomic.Int32
	last  []*events.SerializedEvent
}

func (c *capturingNotifier) NotifyCommitted(_ context.Context, evs []*events.SerializedEvent) {
	c.calls.Add(1)
	c.last = evs
}

func newTestOutbox(t *testing.T) *Outbox {
	t.Helper()
	ob, err := New(&Options{
		TableName:  "t_outbox",
		Serializer: serialization.NewJSONSerializer(),
	})
	assert.NoError(t, err)
	return ob
}

func TestOutboxNotifyCommittedNoopWithoutNotifier(t *testing.T) {
	ob := newTestOutbox(t)
	// Must not panic and must silently no-op.
	ob.NotifyCommitted(context.Background(), []*events.SerializedEvent{
		{Metadata: &events.EventMetadata{Id: events.EventID("abc"), Topic: "t"}},
	})
}

func TestOutboxNotifyCommittedDelegates(t *testing.T) {
	ob := newTestOutbox(t)
	n := &capturingNotifier{}
	ob.SetNotifier(n)

	evs := []*events.SerializedEvent{
		{Metadata: &events.EventMetadata{Id: events.EventID("1"), Topic: "t"}},
		{Metadata: &events.EventMetadata{Id: events.EventID("2"), Topic: "t"}},
	}
	ob.NotifyCommitted(context.Background(), evs)
	assert.Equal(t, int32(1), n.calls.Load())
	assert.Equal(t, 2, len(n.last))
}

func TestOutboxNotifyCommittedEmptySliceSkipped(t *testing.T) {
	ob := newTestOutbox(t)
	n := &capturingNotifier{}
	ob.SetNotifier(n)
	ob.NotifyCommitted(context.Background(), nil)
	assert.Equal(t, int32(0), n.calls.Load())
}

func TestOutboxSetNotifierClear(t *testing.T) {
	ob := newTestOutbox(t)
	n := &capturingNotifier{}
	ob.SetNotifier(n)
	ob.SetNotifier(nil)
	ob.NotifyCommitted(context.Background(), []*events.SerializedEvent{
		{Metadata: &events.EventMetadata{Id: events.EventID("1"), Topic: "t"}},
	})
	assert.Equal(t, int32(0), n.calls.Load())
}
