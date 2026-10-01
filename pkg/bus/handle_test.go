package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

type testEvent struct {
	Name string `json:"name"`
}

func newRoutedSubscription(deadLetter DeadLetterFunc, options ...SubscribeOption) *Subscription {
	subscriptionOptions := SubscriptionOptions{}
	for _, option := range options {
		option(&subscriptionOptions)
	}

	return NewSubscriptionWithSettings(make(chan InboundMessage), SubscriptionSettings{
		Deserializer: serialization.NewJSONSerializer(),
		RetryPolicy:  testRetryPolicy,
		DeadLetter:   deadLetter,
		Routes:       subscriptionOptions.Routes,
	})
}

func TestHandleDecodesPayloadIntoTypedEvent(t *testing.T) {
	var received *testEvent
	var receivedId string
	sub := newRoutedSubscription(nil, Handle("incidents.v1.created", func(ctx context.Context, event *testEvent, message InboundMessage) error {
		received = event
		receivedId = message.Id
		return nil
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	acks, _, _ := recorder.settled()
	assert.Equal(t, 1, acks)
	if assert.NotNil(t, received) {
		assert.Equal(t, "test", received.Name)
	}
	assert.Equal(t, "msg-1", receivedId)
}

func TestHandleDeadLettersUndecodablePayloadWithoutCallingHandler(t *testing.T) {
	dl := &deadLetterRecorder{}
	called := false
	sub := newRoutedSubscription(dl.deadLetter, Handle("incidents.v1.created", func(ctx context.Context, event *testEvent, message InboundMessage) error {
		called = true
		return nil
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	message.Data = []byte("not json")
	sub.handleMessage(message)

	_, terms, nakDelays := recorder.settled()
	assert.False(t, called)
	assert.Equal(t, 1, terms)
	assert.Empty(t, nakDelays)
	if assert.Len(t, dl.calls, 1) {
		assert.True(t, IsPermanent(dl.calls[0].cause))
	}
}

func TestValidateRoutesReturnsPatternsAsFilterSubjects(t *testing.T) {
	noop := func(ctx context.Context, message InboundMessage) error { return nil }

	patterns, err := ValidateRoutes([]Route{
		{Pattern: "incidents.v1.incident.*.created", Handler: noop},
		{Pattern: "tasks.v1.task.>", Handler: noop},
	})

	assert.NoError(t, err)
	assert.Equal(t, []string{"incidents.v1.incident.*.created", "tasks.v1.task.>"}, patterns)
}

func TestValidateRoutesRejectsInvalidRoutes(t *testing.T) {
	noop := func(ctx context.Context, message InboundMessage) error { return nil }

	tests := map[string][]Route{
		"duplicate pattern": {{Pattern: "incidents.>", Handler: noop}, {Pattern: "incidents.>", Handler: noop}},
		"empty pattern":     {{Pattern: "", Handler: noop}},
		"invalid pattern":   {{Pattern: "incidents.>.created", Handler: noop}},
		"missing handler":   {{Pattern: "incidents.>"}},
	}

	for name, routes := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ValidateRoutes(routes)
			assert.ErrorIs(t, err, ErrInvalidRoutes)
		})
	}
}

func TestAddHandlerIsRejectedWhenHandlersAreDeclared(t *testing.T) {
	sub := newRoutedSubscription(nil, HandleRaw("incidents.v1.created", func(ctx context.Context, message InboundMessage) error {
		return nil
	}))

	err := sub.AddHandler("incidents.v1.deleted", func(ctx context.Context, message InboundMessage) error {
		return nil
	})

	assert.ErrorIs(t, err, ErrHandlerRegistrationFailed)
}

func TestSkipAcksMessageWithoutRetry(t *testing.T) {
	dl := &deadLetterRecorder{}
	sub := newRoutedSubscription(dl.deadLetter, HandleRaw("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return Skip("incident %s was deleted", "01J")
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Equal(t, 1, acks)
	assert.Zero(t, terms)
	assert.Empty(t, nakDelays)
	assert.Empty(t, dl.calls)
}

func TestSkipSurvivesWrapping(t *testing.T) {
	err := errors.Join(errors.New("context"), Skip("gone"))

	assert.True(t, IsSkip(err))
	assert.False(t, IsSkip(errors.New("gone")))
}

func TestFailingHandlerRetriesMessageEvenWhenAnotherSkipped(t *testing.T) {
	sub := NewSubscriptionWithSettings(make(chan InboundMessage), SubscriptionSettings{
		Deserializer: serialization.NewJSONSerializer(),
		RetryPolicy:  testRetryPolicy,
	})
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return Skip("not relevant")
	}))
	assert.NoError(t, sub.AddHandler("incidents.v1.*", func(ctx context.Context, message InboundMessage) error {
		return errors.New("downstream unavailable")
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	acks, _, nakDelays := recorder.settled()
	assert.Zero(t, acks)
	assert.Len(t, nakDelays, 1)
}

type middlewareKey struct{}

func TestMiddlewareWrapsHandlersOutermostFirst(t *testing.T) {
	var calls []string
	trace := func(name string) Middleware {
		return func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, message InboundMessage) error {
				calls = append(calls, name)
				return next(context.WithValue(ctx, middlewareKey{}, name), message)
			}
		}
	}

	var seen any
	sub := NewSubscriptionWithSettings(make(chan InboundMessage), SubscriptionSettings{
		Deserializer: serialization.NewJSONSerializer(),
		RetryPolicy:  testRetryPolicy,
		Middleware:   []Middleware{trace("outer"), trace("inner")},
		Routes: []Route{{Pattern: "incidents.>", Handler: func(ctx context.Context, message InboundMessage) error {
			seen = ctx.Value(middlewareKey{})
			return nil
		}}},
	})

	message, _ := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	assert.Equal(t, []string{"outer", "inner"}, calls)
	assert.Equal(t, "inner", seen)
}

func TestMiddlewareAlsoWrapsAddedHandlers(t *testing.T) {
	wrapped := false
	sub := NewSubscriptionWithSettings(make(chan InboundMessage), SubscriptionSettings{
		Deserializer: serialization.NewJSONSerializer(),
		RetryPolicy:  testRetryPolicy,
		Middleware: []Middleware{func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, message InboundMessage) error {
				wrapped = true
				return next(ctx, message)
			}
		}},
	})
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		return nil
	}))

	message, _ := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	assert.True(t, wrapped)
}

func TestHandlerTimeoutCancelsHandlerAndRetriesMessage(t *testing.T) {
	sub := NewSubscriptionWithSettings(make(chan InboundMessage), SubscriptionSettings{
		Deserializer:   serialization.NewJSONSerializer(),
		RetryPolicy:    testRetryPolicy,
		HandlerTimeout: 20 * time.Millisecond,
	})
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		<-ctx.Done()
		return ctx.Err()
	}))

	message, recorder := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	acks, terms, nakDelays := recorder.settled()
	assert.Zero(t, acks)
	assert.Zero(t, terms)
	assert.Len(t, nakDelays, 1)
}

func TestHandlerHasNoDeadlineWithoutTimeout(t *testing.T) {
	var hasDeadline bool
	sub := newSettleSubscription(nil)
	assert.NoError(t, sub.AddHandler("incidents.>", func(ctx context.Context, message InboundMessage) error {
		_, hasDeadline = ctx.Deadline()
		return nil
	}))

	message, _ := newRecordedMessage("incidents.v1.created", 1)
	sub.handleMessage(message)

	assert.False(t, hasDeadline)
}

func TestUnmarshalWithoutDeserializerIsPermanent(t *testing.T) {
	message := InboundMessage{Data: []byte(`{}`)}

	err := message.Unmarshal(&testEvent{})

	assert.True(t, IsPermanent(err))
}
