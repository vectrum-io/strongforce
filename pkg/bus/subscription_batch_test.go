package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

type batchEvent struct {
	Name string `json:"name"`
}

type batchMessageRecorder struct {
	*settleRecorder
	inProgress atomic.Int32
}

func newBatchMessage(name string, numDelivered uint64) (InboundMessage, *batchMessageRecorder) {
	message, recorder := newRecordedMessage("aggregation.created."+name, numDelivered)
	message.Id = name
	message.Data = []byte(fmt.Sprintf(`{"name":%q}`, name))
	batchRecorder := &batchMessageRecorder{settleRecorder: recorder}
	message.InProgress = func() error {
		batchRecorder.inProgress.Add(1)
		return nil
	}
	return message, batchRecorder
}

type syncDeadLetters struct {
	mu    sync.Mutex
	calls []deadLetterCall
}

func (d *syncDeadLetters) deadLetter(_ context.Context, message InboundMessage, cause error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, deadLetterCall{message: message, cause: cause})
	return nil
}

func (d *syncDeadLetters) ids() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := make([]string, len(d.calls))
	for i, call := range d.calls {
		ids[i] = call.message.Id
	}
	return ids
}

type batchSubscriptionSettings struct {
	size       int
	wait       time.Duration
	heartbeat  time.Duration
	deadLetter DeadLetterFunc
	middleware []Middleware
	pinned     func() bool
	unpin      func(ctx context.Context) error
}

func newBatchSubscription(t *testing.T, cfg batchSubscriptionSettings, handler func(ctx context.Context, messages []Message[batchEvent]) []error) (*Subscription, chan InboundMessage) {
	t.Helper()
	if cfg.size == 0 {
		cfg.size = 10
	}
	if cfg.wait == 0 {
		cfg.wait = 20 * time.Millisecond
	}
	if cfg.heartbeat == 0 {
		cfg.heartbeat = time.Hour
	}

	options := SubscriptionOptions{}
	HandleBatch("aggregation.created.>", handler, BatchSize(cfg.size), BatchWait(cfg.wait))(&options)
	require.NoError(t, options.ValidateBatch())

	inbound := make(chan InboundMessage, 100)
	sub := NewSubscriptionWithSettings(inbound, SubscriptionSettings{
		Deserializer:      serialization.NewJSONSerializer(),
		RetryPolicy:       testRetryPolicy,
		DeadLetter:        cfg.deadLetter,
		Stream:            "alert-aggregation",
		Consumer:          "alerts-aggregation-p00",
		Middleware:        cfg.middleware,
		Batch:             options.Batch,
		HeartbeatInterval: cfg.heartbeat,
		Pinned:            cfg.pinned,
		Unpin:             cfg.unpin,
	})
	return sub, inbound
}

func names(messages []Message[batchEvent]) []string {
	result := make([]string, len(messages))
	for i, message := range messages {
		result[i] = message.Event.Name
	}
	return result
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	assert.Eventually(t, condition, 2*time.Second, 5*time.Millisecond)
}

func TestBatchHandlerReceivesFullBatchWithoutWaiting(t *testing.T) {
	batches := make(chan []string, 10)
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 3, wait: time.Hour}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		batches <- names(messages)
		return make([]error, len(messages))
	})

	for _, name := range []string{"a", "b", "c"} {
		message, _ := newBatchMessage(name, 1)
		inbound <- message
	}
	sub.Start(context.Background())

	select {
	case batch := <-batches:
		assert.Equal(t, []string{"a", "b", "c"}, batch)
	case <-time.After(2 * time.Second):
		t.Fatal("full batch was not handled")
	}
}

func TestBatchHandlerReceivesPartialBatchAfterWait(t *testing.T) {
	batches := make(chan []string, 10)
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 10, wait: 30 * time.Millisecond}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		batches <- names(messages)
		return make([]error, len(messages))
	})
	sub.Start(context.Background())

	start := time.Now()
	message, _ := newBatchMessage("a", 1)
	inbound <- message

	select {
	case batch := <-batches:
		assert.Equal(t, []string{"a"}, batch)
		assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("partial batch was not handled")
	}
}

func TestBatchesAreHandledOneAtATime(t *testing.T) {
	release := make(chan struct{})
	var running, maxRunning atomic.Int32
	batches := make(chan []string, 10)

	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 2}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		now := running.Add(1)
		if now > maxRunning.Load() {
			maxRunning.Store(now)
		}
		batches <- names(messages)
		<-release
		running.Add(-1)
		return make([]error, len(messages))
	})
	sub.Start(context.Background())

	for _, name := range []string{"a", "b", "c", "d"} {
		message, _ := newBatchMessage(name, 1)
		inbound <- message
	}

	assert.Equal(t, []string{"a", "b"}, <-batches)
	select {
	case batch := <-batches:
		t.Fatalf("second batch %v started while the first one was running", batch)
	case <-time.After(50 * time.Millisecond):
	}

	release <- struct{}{}
	assert.Equal(t, []string{"c", "d"}, <-batches)
	release <- struct{}{}

	assert.Equal(t, int32(1), maxRunning.Load())
}

func TestBatchSettlesEachMessageByItsOutcome(t *testing.T) {
	dl := &syncDeadLetters{}
	handled := make(chan struct{})
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 4, deadLetter: dl.deadLetter}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		defer close(handled)
		return []error{
			nil,
			Skip("processor deleted"),
			Permanent(errors.New("broken payload")),
			errors.New("database unavailable"),
		}
	})

	ok, okRec := newBatchMessage("ok", 1)
	skipped, skippedRec := newBatchMessage("skipped", 1)
	permanent, permanentRec := newBatchMessage("permanent", 1)
	retried, retriedRec := newBatchMessage("retried", 2)
	for _, message := range []InboundMessage{ok, skipped, permanent, retried} {
		inbound <- message
	}
	sub.Start(context.Background())
	<-handled

	eventually(t, func() bool {
		_, _, nakDelays := retriedRec.settled()
		return len(nakDelays) == 1
	})

	acks, _, _ := okRec.settled()
	assert.Equal(t, 1, acks)
	acks, _, _ = skippedRec.settled()
	assert.Equal(t, 1, acks)
	_, terms, _ := permanentRec.settled()
	assert.Equal(t, 1, terms)
	_, _, nakDelays := retriedRec.settled()
	assert.Equal(t, []time.Duration{2 * time.Second}, nakDelays)
	assert.Equal(t, []string{"permanent"}, dl.ids())
}

func TestBatchDeadLettersUndecodablePayloadAndHandlesTheRest(t *testing.T) {
	dl := &syncDeadLetters{}
	batches := make(chan []string, 1)
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 2, deadLetter: dl.deadLetter}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		batches <- names(messages)
		return make([]error, len(messages))
	})

	broken, brokenRec := newBatchMessage("broken", 1)
	broken.Data = []byte("not json")
	good, goodRec := newBatchMessage("good", 1)
	inbound <- broken
	inbound <- good
	sub.Start(context.Background())

	assert.Equal(t, []string{"good"}, <-batches)
	eventually(t, func() bool {
		acks, _, _ := goodRec.settled()
		_, terms, _ := brokenRec.settled()
		return acks == 1 && terms == 1
	})
	assert.Equal(t, []string{"broken"}, dl.ids())
}

func TestBatchRetriesEveryMessageWhenOutcomeCountIsWrong(t *testing.T) {
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 2}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		return nil
	})

	a, aRec := newBatchMessage("a", 1)
	b, bRec := newBatchMessage("b", 1)
	inbound <- a
	inbound <- b
	sub.Start(context.Background())

	eventually(t, func() bool {
		_, _, aNaks := aRec.settled()
		_, _, bNaks := bRec.settled()
		return len(aNaks) == 1 && len(bNaks) == 1
	})
}

func TestBatchRetriesEveryMessageWhenHandlerPanics(t *testing.T) {
	var reported atomic.Int32
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 2}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		panic("correlator exploded")
	})
	sub.OnError(func(err error) { reported.Add(1) })

	a, aRec := newBatchMessage("a", 1)
	b, bRec := newBatchMessage("b", 1)
	inbound <- a
	inbound <- b
	sub.Start(context.Background())

	eventually(t, func() bool {
		_, _, aNaks := aRec.settled()
		_, _, bNaks := bRec.settled()
		return len(aNaks) == 1 && len(bNaks) == 1
	})
	assert.Equal(t, int32(1), reported.Load(), "one failure of a batch is reported once")
}

func TestBatchDeadLettersExhaustedMessageWithoutHandlingIt(t *testing.T) {
	dl := &syncDeadLetters{}
	batches := make(chan []string, 1)
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 1, deadLetter: dl.deadLetter}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		batches <- names(messages)
		return make([]error, len(messages))
	})

	exhausted, _ := newBatchMessage("exhausted", 4)
	fresh, _ := newBatchMessage("fresh", 1)
	inbound <- exhausted
	inbound <- fresh
	sub.Start(context.Background())

	assert.Equal(t, []string{"fresh"}, <-batches)
	eventually(t, func() bool { return len(dl.ids()) == 1 })
	assert.Equal(t, []string{"exhausted"}, dl.ids())
}

func TestBatchKeepsRunningAndWaitingMessagesInProgress(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 1, heartbeat: 10 * time.Millisecond}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		if messages[0].Event.Name == "running" {
			close(started)
			<-release
		}
		return make([]error, len(messages))
	})
	sub.Start(context.Background())

	running, runningRec := newBatchMessage("running", 1)
	inbound <- running
	<-started
	waiting, waitingRec := newBatchMessage("waiting", 1)
	inbound <- waiting

	eventually(t, func() bool {
		return runningRec.inProgress.Load() >= 2 && waitingRec.inProgress.Load() >= 2
	})
	close(release)
}

func TestBatchMiddlewareRunsOncePerBatch(t *testing.T) {
	type ctxKey struct{}
	var calls atomic.Int32
	var seen InboundMessage
	middleware := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, message InboundMessage) error {
			calls.Add(1)
			seen = message
			return next(context.WithValue(ctx, ctxKey{}, "service"), message)
		}
	}

	values := make(chan any, 1)
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 2, middleware: []Middleware{middleware}}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		values <- ctx.Value(ctxKey{})
		return make([]error, len(messages))
	})

	a, _ := newBatchMessage("a", 1)
	b, _ := newBatchMessage("b", 1)
	inbound <- a
	inbound <- b
	sub.Start(context.Background())

	assert.Equal(t, "service", <-values)
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, "aggregation.created.>", seen.Subject)
	assert.Equal(t, "alert-aggregation", seen.Delivery.Stream)
	assert.Equal(t, "alerts-aggregation-p00", seen.Delivery.Consumer)
}

func TestShutdownFinishesRunningBatchReleasesWaitingMessagesAndUnpins(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var unpinned atomic.Bool
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{
		size:   1,
		pinned: func() bool { return !unpinned.Load() },
		unpin: func(ctx context.Context) error {
			unpinned.Store(true)
			return nil
		},
	}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		if messages[0].Event.Name == "running" {
			close(started)
			<-release
		}
		return make([]error, len(messages))
	})
	sub.Start(context.Background())

	running, runningRec := newBatchMessage("running", 1)
	inbound <- running
	<-started
	waiting, waitingRec := newBatchMessage("waiting", 1)
	inbound <- waiting

	shutdown := make(chan error, 1)
	go func() { shutdown <- sub.Shutdown(context.Background()) }()

	select {
	case <-shutdown:
		t.Fatal("shutdown returned while a batch was running")
	case <-time.After(50 * time.Millisecond):
	}
	assert.False(t, unpinned.Load(), "the pin is kept until the running batch is settled")

	close(release)
	assert.NoError(t, <-shutdown)

	acks, _, _ := runningRec.settled()
	assert.Equal(t, 1, acks)
	waitingAcks, _, waitingNaks := waitingRec.settled()
	assert.Zero(t, waitingAcks)
	assert.Equal(t, []time.Duration{0}, waitingNaks)
	assert.True(t, unpinned.Load())
}

func TestShutdownDoesNotUnpinWithoutPin(t *testing.T) {
	var unpinCalls atomic.Int32
	sub, _ := newBatchSubscription(t, batchSubscriptionSettings{
		pinned: func() bool { return false },
		unpin: func(ctx context.Context) error {
			unpinCalls.Add(1)
			return nil
		},
	}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		return make([]error, len(messages))
	})
	sub.Start(context.Background())

	assert.NoError(t, sub.Shutdown(context.Background()))
	assert.Zero(t, unpinCalls.Load())
}

func TestShutdownCancelsBatchWhenContextEnds(t *testing.T) {
	started := make(chan struct{})
	sub, inbound := newBatchSubscription(t, batchSubscriptionSettings{size: 1}, func(ctx context.Context, messages []Message[batchEvent]) []error {
		close(started)
		<-ctx.Done()
		return []error{ctx.Err()}
	})
	sub.Start(context.Background())

	message, recorder := newBatchMessage("slow", 1)
	inbound <- message
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.NoError(t, sub.Shutdown(ctx))

	_, _, nakDelays := recorder.settled()
	assert.Len(t, nakDelays, 1)
}

func TestShutdownWaitsForRunningMessageHandlers(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	sub := newSettleSubscription(nil)
	inbound := make(chan InboundMessage, 1)
	sub.inboundMessages = inbound
	var stopped atomic.Bool
	sub.unsubscribe = func() { stopped.Store(true) }
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		close(started)
		<-release
		return nil
	}))
	sub.Start(context.Background())

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	inbound <- message
	<-started

	shutdown := make(chan error, 1)
	go func() { shutdown <- sub.Shutdown(context.Background()) }()
	eventually(t, stopped.Load)

	select {
	case <-shutdown:
		t.Fatal("shutdown returned while a handler was running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	assert.NoError(t, <-shutdown)
	acks, _, _ := recorder.settled()
	assert.Equal(t, 1, acks)
}

func TestValidateBatchRejectsInvalidSubscriptions(t *testing.T) {
	handler := func(ctx context.Context, messages []InboundMessage) []error { return nil }
	tests := map[string]SubscriptionOptions{
		"batch combined with a route": {
			Batch:  &BatchRoute{Pattern: "a.>", Handler: handler, Size: 1, Wait: time.Second},
			Routes: []Route{{Pattern: "b.>"}},
		},
		"zero batch size": {
			Batch: &BatchRoute{Pattern: "a.>", Handler: handler, Size: 0, Wait: time.Second},
		},
		"zero batch wait": {
			Batch: &BatchRoute{Pattern: "a.>", Handler: handler, Size: 1},
		},
		"pinned group without batch": {
			PinnedGroup: &PinnedGroup{Group: "g", TTL: time.Second},
		},
		"pinned group without TTL": {
			Batch:       &BatchRoute{Pattern: "a.>", Handler: handler, Size: 1, Wait: time.Second},
			PinnedGroup: &PinnedGroup{Group: "g"},
		},
	}

	for name, options := range tests {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, options.ValidateBatch(), ErrInvalidBatch)
		})
	}
}

func TestStartAfterShutdownDoesNothing(t *testing.T) {
	var receiving atomic.Bool
	options := SubscriptionOptions{}
	HandleBatch("aggregation.created.>", func(ctx context.Context, messages []Message[batchEvent]) []error {
		return make([]error, len(messages))
	})(&options)

	sub := NewSubscriptionWithSettings(make(chan InboundMessage), SubscriptionSettings{
		Batch:             options.Batch,
		HeartbeatInterval: time.Hour,
		StartReceiving:    func(ctx context.Context) { receiving.Store(true) },
	})

	assert.NoError(t, sub.Shutdown(context.Background()))
	sub.Start(context.Background())

	assert.False(t, receiving.Load())
	assert.False(t, sub.IsRunning())
}
