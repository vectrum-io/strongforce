package bus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

// settleRecorder captures how a subscription settled a message.
type settleRecorder struct {
	mu        sync.Mutex
	acks      int
	terms     int
	nakDelays []time.Duration
}

func (r *settleRecorder) settled() (acks int, terms int, nakDelays []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acks, r.terms, append([]time.Duration(nil), r.nakDelays...)
}

func newRecordedMessage(subject string, numDelivered uint64) (InboundMessage, *settleRecorder) {
	recorder := &settleRecorder{}
	message := InboundMessage{
		MessageCtx: context.Background(),
		Id:         "msg-1",
		Subject:    subject,
		Data:       []byte(`{"name":"test"}`),
		Delivery: DeliveryInfo{
			Stream:         "incidents",
			Consumer:       "notifications",
			StreamSequence: 42,
			NumDelivered:   numDelivered,
		},
		Ack: func() error {
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			recorder.acks++
			return nil
		},
		Nak: func(delay time.Duration) error {
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			recorder.nakDelays = append(recorder.nakDelays, delay)
			return nil
		},
		Term: func() error {
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			recorder.terms++
			return nil
		},
	}
	return message, recorder
}

type deadLetterCall struct {
	message InboundMessage
	cause   error
}

type deadLetterRecorder struct {
	calls []deadLetterCall
	err   error
}

func (d *deadLetterRecorder) deadLetter(_ context.Context, message InboundMessage, cause error) error {
	d.calls = append(d.calls, deadLetterCall{message: message, cause: cause})
	return d.err
}

var testRetryPolicy = RetryPolicy{
	MaxAttempts:  3,
	InitialDelay: time.Second,
	MaxDelay:     10 * time.Second,
	Multiplier:   2,
}

func newSettleSubscription(deadLetter DeadLetterFunc) *Subscription {
	return NewSubscriptionWithSettings(make(chan InboundMessage), SubscriptionSettings{
		Deserializer: serialization.NewJSONSerializer(),
		RetryPolicy:  testRetryPolicy,
		DeadLetter:   deadLetter,
		Stream:       "incidents",
		Consumer:     "notifications",
	})
}

func TestSettleNaksFailedMessageWithBackoffForItsAttempt(t *testing.T) {
	dl := &deadLetterRecorder{}
	sub := newSettleSubscription(dl.deadLetter)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return errors.New("downstream unavailable")
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 2)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Zero(t, acks)
	assert.Zero(t, terms)
	assert.Equal(t, []time.Duration{2 * time.Second}, nakDelays)
	assert.Empty(t, dl.calls)
}

func TestSettleDeadLettersAndTerminatesOnLastAttempt(t *testing.T) {
	dl := &deadLetterRecorder{}
	sub := newSettleSubscription(dl.deadLetter)
	handlerErr := errors.New("downstream unavailable")
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return handlerErr
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 3)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Zero(t, acks)
	assert.Equal(t, 1, terms)
	assert.Empty(t, nakDelays)
	if assert.Len(t, dl.calls, 1) {
		assert.Equal(t, "incidents.v1.created", dl.calls[0].message.Subject)
		assert.ErrorIs(t, dl.calls[0].cause, handlerErr)
		assert.ErrorIs(t, dl.calls[0].cause, ErrMessageHandlerFailed)
	}
}

func TestSettleDeadLettersPermanentErrorOnFirstAttempt(t *testing.T) {
	dl := &deadLetterRecorder{}
	sub := newSettleSubscription(dl.deadLetter)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return Permanent(errors.New("incident does not exist"))
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	_, terms, nakDelays := recorder.settled()
	assert.Equal(t, 1, terms)
	assert.Empty(t, nakDelays)
	assert.Len(t, dl.calls, 1)
}

func TestSettleTreatsUndecodablePayloadAsPermanent(t *testing.T) {
	dl := &deadLetterRecorder{}
	sub := newSettleSubscription(dl.deadLetter)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		var dst struct{ Name string }
		return message.Unmarshal(&dst)
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	message.Data = []byte("not json")
	sub.handleMessage(message)

	_, terms, nakDelays := recorder.settled()
	assert.Equal(t, 1, terms)
	assert.Empty(t, nakDelays)
	assert.Len(t, dl.calls, 1)
}

func TestSettleDropsExhaustedMessageWithoutDeadLetter(t *testing.T) {
	sub := newSettleSubscription(nil)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return errors.New("stale heartbeat")
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 3)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Zero(t, acks)
	assert.Equal(t, 1, terms)
	assert.Empty(t, nakDelays)
}

func TestSettleKeepsMessageWhenDeadLetteringFails(t *testing.T) {
	dl := &deadLetterRecorder{err: errors.New("dead letter stream unavailable")}
	sub := newSettleSubscription(dl.deadLetter)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return errors.New("downstream unavailable")
	}))

	var reported []error
	sub.OnError(func(err error) {
		reported = append(reported, err)
	})

	message, recorder := newRecordedMessage("incidents.v1.created", 3)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Zero(t, acks)
	assert.Zero(t, terms)
	assert.Equal(t, []time.Duration{testRetryPolicy.MaxDelay}, nakDelays)
	assert.Len(t, dl.calls, 1)
	if assert.Len(t, reported, 2) {
		assert.ErrorContains(t, reported[1], "failed to dead-letter message")
	}
}

func TestSettleDeadLettersWithoutRunningHandlersPastDeliveryLimit(t *testing.T) {
	dl := &deadLetterRecorder{}
	sub := newSettleSubscription(dl.deadLetter)
	handlerCalled := false
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		handlerCalled = true
		return nil
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 4)
	sub.handleMessage(message)

	acks, terms, _ := recorder.settled()
	assert.False(t, handlerCalled)
	assert.Zero(t, acks)
	assert.Equal(t, 1, terms)
	if assert.Len(t, dl.calls, 1) {
		assert.ErrorIs(t, dl.calls[0].cause, ErrDeliveryLimitExceeded)
	}
}

func TestSettleAcksMessageWithoutMatchingHandler(t *testing.T) {
	dl := &deadLetterRecorder{}
	sub := newSettleSubscription(dl.deadLetter)
	assert.NoError(t, sub.AddHandler("incidents.v1.created", func(ctx context.Context, message InboundMessage) error {
		return nil
	}))

	var reported []error
	sub.OnError(func(err error) {
		reported = append(reported, err)
	})

	message, recorder := newRecordedMessage("incidents.v1.api_token_created", 1)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Equal(t, 1, acks)
	assert.Zero(t, terms)
	assert.Empty(t, nakDelays)
	assert.Empty(t, reported)
	assert.Empty(t, dl.calls)
}

func TestSettleAcksSuccessfulMessage(t *testing.T) {
	sub := newSettleSubscription(nil)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return nil
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Equal(t, 1, acks)
	assert.Zero(t, terms)
	assert.Empty(t, nakDelays)
}

func TestSettleRetriesWhenAnyOfSeveralHandlersFails(t *testing.T) {
	sub := newSettleSubscription(nil)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return nil
	}))
	assert.NoError(t, sub.AddHandler("incidents.v1.*", func(ctx context.Context, message InboundMessage) error {
		return errors.New("downstream unavailable")
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	acks, _, nakDelays := recorder.settled()
	assert.Zero(t, acks)
	assert.Equal(t, []time.Duration{time.Second}, nakDelays)
}
