package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	nats2 "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
)

type batchTestEvent struct {
	Name string `json:"name"`
}

// batchReceiver records the batches a subscriber handled.
type batchReceiver struct {
	name    string
	mu      sync.Mutex
	names   []string
	batches chan []string
	hold    time.Duration
}

func newBatchReceiver(name string) *batchReceiver {
	return &batchReceiver{name: name, batches: make(chan []string, 100)}
}

func (r *batchReceiver) handle(ctx context.Context, messages []bus.Message[batchTestEvent]) []error {
	names := make([]string, len(messages))
	for i, message := range messages {
		names[i] = message.Event.Name
	}
	r.mu.Lock()
	r.names = append(r.names, names...)
	hold := r.hold
	r.mu.Unlock()
	r.batches <- names
	time.Sleep(hold)
	return make([]error, len(messages))
}

func (r *batchReceiver) received() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.names...)
}

func (r *batchReceiver) setHold(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hold = d
}

func newBatchStream(t *testing.T) (stream string, subject string) {
	t.Helper()
	stream = fmt.Sprintf("test-batch-%d", time.Now().UnixNano())
	require.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, stream, stream+".>"))
	return stream, stream + ".created"
}

func publishBatchEvents(t *testing.T, natsBus *nats.Bus, subject string, names ...string) {
	t.Helper()
	for _, name := range names {
		require.NoError(t, natsBus.Publish(context.Background(), &bus.OutboundMessage{
			Id:      fmt.Sprintf("%s-%s", subject, name),
			Subject: subject,
			Data:    []byte(fmt.Sprintf(`{"name":%q}`, name)),
		}))
	}
}

func subscribePinned(t *testing.T, natsBus *nats.Bus, consumer, stream, subject string, receiver *batchReceiver, ttl time.Duration, opts ...bus.SubscribeOption) *bus.Subscription {
	t.Helper()
	opts = append([]bus.SubscribeOption{
		bus.WithDeserializer(serialization.NewJSONSerializer()),
		bus.HandleBatch(subject, receiver.handle, bus.BatchSize(10), bus.BatchWait(50*time.Millisecond)),
		bus.WithDurable(),
		bus.WithPinnedPriorityGroup("aggregation", ttl),
	}, opts...)
	subscription, err := natsBus.Subscribe(context.Background(), consumer, stream, opts...)
	require.NoError(t, err)
	subscription.Start(context.Background())
	return subscription
}

func waitForBatch(t *testing.T, receiver *batchReceiver, timeout time.Duration) []string {
	t.Helper()
	select {
	case batch := <-receiver.batches:
		return batch
	case <-time.After(timeout):
		t.Fatalf("%s handled no batch within %s", receiver.name, timeout)
		return nil
	}
}

// pinnedOf returns which of the subscriptions holds the pin once one does.
func pinnedOf(t *testing.T, subscriptions map[string]*bus.Subscription) string {
	t.Helper()
	var holder string
	assert.Eventually(t, func() bool {
		for name, subscription := range subscriptions {
			if subscription.IsPinned() {
				holder = name
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
	return holder
}

func consumerInfo(t *testing.T, stream, consumer string) *nats2.ConsumerInfo {
	t.Helper()
	nc, err := nats2.Connect(sharedtest.NATS)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require.NoError(t, err)
	info, err := js.ConsumerInfo(stream, consumer)
	require.NoError(t, err)
	return info
}

func TestBatchSubscriptionHandlesAndAcksMessages(t *testing.T) {
	stream, subject := newBatchStream(t)
	natsBus := newMigratedBus(t)
	receiver := newBatchReceiver("only")

	subscription, err := natsBus.Subscribe(context.Background(), stream, stream,
		bus.WithDeserializer(serialization.NewJSONSerializer()),
		bus.HandleBatch(subject, receiver.handle, bus.BatchSize(3), bus.BatchWait(time.Hour)),
		bus.WithDurable(),
	)
	require.NoError(t, err)
	publishBatchEvents(t, natsBus, subject, "a", "b", "c")
	subscription.Start(context.Background())

	assert.ElementsMatch(t, []string{"a", "b", "c"}, waitForBatch(t, receiver, 5*time.Second))
	assert.Eventually(t, func() bool {
		info := consumerInfo(t, stream, stream)
		return info.NumAckPending == 0 && info.NumPending == 0
	}, 5*time.Second, 20*time.Millisecond)
	assert.NoError(t, subscription.Shutdown(context.Background()))
}

func TestPinnedGroupDeliversToOneSubscriberOnly(t *testing.T) {
	stream, subject := newBatchStream(t)
	natsBus := newMigratedBus(t)
	first, second := newBatchReceiver("first"), newBatchReceiver("second")

	subscriptions := map[string]*bus.Subscription{
		"first":  subscribePinned(t, natsBus, stream, stream, subject, first, 5*time.Second),
		"second": subscribePinned(t, newMigratedBus(t), stream, stream, subject, second, 5*time.Second),
	}
	t.Cleanup(func() {
		for _, subscription := range subscriptions {
			_ = subscription.Shutdown(context.Background())
		}
	})

	for i := range 5 {
		publishBatchEvents(t, natsBus, subject, fmt.Sprintf("m%d", i))
		time.Sleep(100 * time.Millisecond)
	}

	holder := pinnedOf(t, subscriptions)
	assert.Eventually(t, func() bool {
		return len(first.received())+len(second.received()) == 5
	}, 10*time.Second, 20*time.Millisecond)

	if holder == "first" {
		assert.Empty(t, second.received())
	} else {
		assert.Empty(t, first.received())
	}
}

func TestPinMovesRightAwayWhenPinnedSubscriberShutsDown(t *testing.T) {
	stream, subject := newBatchStream(t)
	natsBus := newMigratedBus(t)
	receivers := map[string]*batchReceiver{"first": newBatchReceiver("first"), "second": newBatchReceiver("second")}
	ttl := 30 * time.Second

	subscriptions := map[string]*bus.Subscription{
		"first":  subscribePinned(t, natsBus, stream, stream, subject, receivers["first"], ttl),
		"second": subscribePinned(t, newMigratedBus(t), stream, stream, subject, receivers["second"], ttl),
	}

	publishBatchEvents(t, natsBus, subject, "before")
	holder := pinnedOf(t, subscriptions)
	waitForBatch(t, receivers[holder], 5*time.Second)

	other := "first"
	if holder == "first" {
		other = "second"
	}

	require.NoError(t, subscriptions[holder].Shutdown(context.Background()))
	t.Cleanup(func() { _ = subscriptions[other].Shutdown(context.Background()) })

	publishBatchEvents(t, natsBus, subject, "after")
	assert.Equal(t, []string{"after"}, waitForBatch(t, receivers[other], ttl/2))
}

func TestPinMovesAfterTTLWhenPinnedSubscriberStopsPulling(t *testing.T) {
	stream, subject := newBatchStream(t)
	natsBus := newMigratedBus(t)
	receivers := map[string]*batchReceiver{"first": newBatchReceiver("first"), "second": newBatchReceiver("second")}
	ttl := 2 * time.Second

	subscriptions := map[string]*bus.Subscription{
		"first":  subscribePinned(t, natsBus, stream, stream, subject, receivers["first"], ttl),
		"second": subscribePinned(t, newMigratedBus(t), stream, stream, subject, receivers["second"], ttl),
	}

	publishBatchEvents(t, natsBus, subject, "before")
	holder := pinnedOf(t, subscriptions)
	waitForBatch(t, receivers[holder], 5*time.Second)

	other := "first"
	if holder == "first" {
		other = "second"
	}

	// Stop without unpinning, like a pod that died.
	subscriptions[holder].Stop()
	t.Cleanup(func() { _ = subscriptions[other].Shutdown(context.Background()) })

	publishBatchEvents(t, natsBus, subject, "after")
	assert.Equal(t, []string{"after"}, waitForBatch(t, receivers[other], 5*ttl))
}

func TestPinIsKeptWhileBatchRunsLongerThanTTL(t *testing.T) {
	stream, subject := newBatchStream(t)
	natsBus := newMigratedBus(t)
	receivers := map[string]*batchReceiver{"first": newBatchReceiver("first"), "second": newBatchReceiver("second")}
	ttl := time.Second

	subscriptions := map[string]*bus.Subscription{
		"first":  subscribePinned(t, natsBus, stream, stream, subject, receivers["first"], ttl),
		"second": subscribePinned(t, newMigratedBus(t), stream, stream, subject, receivers["second"], ttl),
	}
	t.Cleanup(func() {
		for _, subscription := range subscriptions {
			_ = subscription.Shutdown(context.Background())
		}
	})

	publishBatchEvents(t, natsBus, subject, "warmup")
	holder := pinnedOf(t, subscriptions)
	waitForBatch(t, receivers[holder], 5*time.Second)

	receivers[holder].setHold(4 * ttl)
	publishBatchEvents(t, natsBus, subject, "slow")
	waitForBatch(t, receivers[holder], 5*time.Second)

	time.Sleep(ttl + ttl/2)
	publishBatchEvents(t, natsBus, subject, "during")
	receivers[holder].setHold(0)

	assert.Equal(t, []string{"during"}, waitForBatch(t, receivers[holder], 10*ttl))
	for name, receiver := range receivers {
		if name != holder {
			assert.Empty(t, receiver.received(), "a second subscriber took over while the pinned one was busy")
		}
	}
}

func TestBatchHeartbeatsPreventRedeliveryOfSlowBatch(t *testing.T) {
	stream, subject := newBatchStream(t)
	natsBus := newMigratedBus(t)
	receiver := newBatchReceiver("slow")
	receiver.setHold(3 * time.Second)

	subscription, err := natsBus.Subscribe(context.Background(), stream, stream,
		bus.WithDeserializer(serialization.NewJSONSerializer()),
		bus.HandleBatch(subject, receiver.handle, bus.BatchSize(1), bus.BatchWait(10*time.Millisecond)),
		bus.WithDurable(),
		bus.WithAckWait(time.Second),
	)
	require.NoError(t, err)
	publishBatchEvents(t, natsBus, subject, "slow")
	subscription.Start(context.Background())
	t.Cleanup(func() { _ = subscription.Shutdown(context.Background()) })

	waitForBatch(t, receiver, 5*time.Second)
	assert.Eventually(t, func() bool {
		return consumerInfo(t, stream, stream).NumAckPending == 0
	}, 10*time.Second, 50*time.Millisecond)

	assert.Equal(t, []string{"slow"}, receiver.received())
	assert.Zero(t, consumerInfo(t, stream, stream).NumRedelivered)
}
