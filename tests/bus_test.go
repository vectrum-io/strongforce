package tests

import (
	"context"
	"errors"
	"fmt"
	nats2 "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	"testing"
	"time"
)

func TestNATSStreamMigrator(t *testing.T) {
	natsBus, err := nats.New(&nats.Options{
		NATSAddress: sharedtest.NATS,
		Streams: []nats2.StreamConfig{
			{
				Name:        "migration-test",
				Description: "migration test stream",
				Subjects: []string{
					"migration.>",
				},
				Retention:    nats2.LimitsPolicy,
				MaxConsumers: -1,
				MaxMsgs:      -1,
				MaxBytes:     -1,
				Discard:      nats2.DiscardOld,
				Storage:      nats2.FileStorage,
				Duplicates:   time.Minute,
			},
			{
				Name:        "migration-test-2",
				Description: "migration test stream 2",
				Subjects: []string{
					"migration-two.*.test",
				},
				Retention:    nats2.LimitsPolicy,
				MaxConsumers: -1,
				MaxMsgs:      -1,
				MaxBytes:     -1,
				Discard:      nats2.DiscardOld,
				Storage:      nats2.FileStorage,
				Duplicates:   time.Minute,
			},
		},
	})
	assert.NoError(t, err)

	migrationErr := natsBus.Migrate(context.Background())
	assert.NoError(t, migrationErr)

	// check nats streams
	testStreamNats, err := sharedtest.GetNATSStream(sharedtest.NATS, "migration-test")
	assert.NoError(t, err)

	assert.Equal(t, "migration-test", testStreamNats.Config.Name)

	testStreamTwoNats, err := sharedtest.GetNATSStream(sharedtest.NATS, "migration-test-2")
	assert.NoError(t, err)

	assert.Equal(t, "migration-test-2", testStreamTwoNats.Config.Name)
}

// TestBusOrderSpamGuaranteed asserts that WithGuaranteeOrder preserves publish
// order end-to-end: 5k messages published in sequence arrive at the handler in
// the same sequence. The guarantee comes from MaxAckPending=1 plus the
// single-goroutine handler — both knobs are flipped on by WithGuaranteeOrder.
func TestBusOrderSpamGuaranteed(t *testing.T) {
	streamName := "test-spam-ordered"
	subject := "test-1-ordered"

	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: sharedtest.NATS,
	})
	assert.NoError(t, err)

	subscription, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName,
		bus.WithFilterSubject(subject), bus.WithGuaranteeOrder())
	assert.NoError(t, err)

	for i := 0; i < 5000; i++ {
		err = natsBus.Publish(context.Background(), &bus.OutboundMessage{
			Id:      fmt.Sprintf("%d", i),
			Subject: subject,
			Data:    []byte(fmt.Sprintf("%d", i)),
		})
		assert.NoError(t, err)
	}

	msgChan := make(chan bus.InboundMessage)
	err = subscription.AddHandler("*", func(ctx context.Context, message bus.InboundMessage) error {
		msgChan <- message
		return nil
	})
	assert.NoError(t, err)

	subscription.Start(context.Background())

	for i := 0; i < 5000; i++ {
		message := <-msgChan
		assert.Equal(t, fmt.Sprintf("%d", i), string(message.Data))
	}
}

// TestBusConcurrentSpam asserts that under the default concurrent dispatch,
// every published message reaches the handler exactly once. Order is not
// asserted — N goroutines race on the inbound channel, so receive order is
// undefined. This is the throughput path most subscribers take; the property
// that matters is "no message dropped, no message duplicated".
func TestBusConcurrentSpam(t *testing.T) {
	streamName := "test-spam-concurrent"
	subject := "test-1-concurrent"

	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: sharedtest.NATS,
	})
	assert.NoError(t, err)

	subscription, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName,
		bus.WithFilterSubject(subject))
	assert.NoError(t, err)

	const total = 5000
	for i := 0; i < total; i++ {
		err = natsBus.Publish(context.Background(), &bus.OutboundMessage{
			Id:      fmt.Sprintf("%d", i),
			Subject: subject,
			Data:    []byte(fmt.Sprintf("%d", i)),
		})
		assert.NoError(t, err)
	}

	msgChan := make(chan bus.InboundMessage, total)
	err = subscription.AddHandler("*", func(ctx context.Context, message bus.InboundMessage) error {
		msgChan <- message
		return nil
	})
	assert.NoError(t, err)

	subscription.Start(context.Background())

	seen := make(map[string]int, total)
	timeout := time.After(30 * time.Second)
	for len(seen) < total {
		select {
		case message := <-msgChan:
			seen[string(message.Data)]++
		case <-timeout:
			t.Fatalf("only received %d/%d messages within timeout", len(seen), total)
		}
	}

	for i := 0; i < total; i++ {
		key := fmt.Sprintf("%d", i)
		assert.Equalf(t, 1, seen[key], "message %s seen %d times", key, seen[key])
	}
}

func TestBusOrderConsumerNak(t *testing.T) {
	streamName := "test-order"
	subject := "test-2"

	// create test stream
	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: sharedtest.NATS,
	})
	assert.NoError(t, err)

	subscriptionA, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName, bus.WithFilterSubject(subject), bus.WithGuaranteeOrder())
	assert.NoError(t, err)

	subscriptionB, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName, bus.WithFilterSubject(subject), bus.WithGuaranteeOrder())
	assert.NoError(t, err)

	// send two messages
	for i := 0; i < 2; i++ {
		t.Logf("send message %d to %s\n", i, subject)
		err = natsBus.Publish(context.Background(), &bus.OutboundMessage{
			Id:      fmt.Sprintf("%d", i),
			Subject: subject,
			Data:    []byte(fmt.Sprintf("%d", i)),
		})
		assert.NoError(t, err)
	}

	// get first one
	t.Log("wait for message 1")
	_, message1, res := waitForMessage(subscriptionA, subscriptionB)
	assert.Equal(t, "0", message1.Id)
	t.Log("nak message 1")
	message1.Nak(0)
	res <- errors.New("failed")

	// expect second message to still be message 0
	t.Log("wait for message 1 retry")
	_, message1Retry, res := waitForMessage(subscriptionA, subscriptionB)
	assert.Equal(t, "0", message1Retry.Id)
	res <- nil

	t.Log("wait for message 2")
	_, message2, res := waitForMessage(subscriptionA, subscriptionB)
	res <- nil
	assert.Equal(t, "1", message2.Id)

}

func TestContextPropagation(t *testing.T) {
	streamName := "test-context-propagation"
	subject := "test-3"

	// create test stream
	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: sharedtest.NATS,
	})
	assert.NoError(t, err)

	type testCtxKey struct{}

	ctx := context.WithValue(context.Background(), testCtxKey{}, "test-val")

	subscription, err := natsBus.Subscribe(ctx, streamName+"-"+subject, streamName, bus.WithFilterSubject(subject), bus.WithGuaranteeOrder())
	assert.NoError(t, err)

	err = natsBus.Publish(context.Background(), &bus.OutboundMessage{
		Id:      "1",
		Subject: subject,
		Data:    []byte("test message with context"),
	})
	assert.NoError(t, err)

	t.Log("wait for message to be received")
	ctx, message, res := waitForMessage(subscription)
	assert.Equal(t, "1", message.Id)
	assert.Equal(t, ctx, message.MessageCtx)
	assert.Equal(t, ctx.Value(testCtxKey{}), "test-val")
	res <- nil
}

func TestSubscribeContextCancelation(t *testing.T) {
	streamName := "test-context-cancel"
	subject := "test-4"

	// create test stream
	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: sharedtest.NATS,
	})
	assert.NoError(t, err)

	subscription, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName, bus.WithFilterSubject(subject), bus.WithGuaranteeOrder())
	assert.NoError(t, err)

	// start subscription

	ctx, cancel := context.WithCancel(context.Background())
	subscription.Start(ctx)

	assert.Equal(t, true, subscription.IsRunning())

	cancel()

	// wait for subscription to stop
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, false, subscription.IsRunning())
}

type HandlerCall struct {
	Ctx     context.Context
	Message bus.InboundMessage
}

// waitForMessage waits for a message to be received on the given subscriptions.
// It returns the received message and a channel that can be used to control
// the handler's return value.
func waitForMessage(subscriptions ...*bus.Subscription) (context.Context, bus.InboundMessage, chan error) {

	resChan := make(chan error)
	msg := make(chan HandlerCall)

	for _, sub := range subscriptions {

		if sub.IsRunning() {
			sub.RemoveHandler(">")
		}

		sub.AddHandler(">", func(ctx context.Context, message bus.InboundMessage) error {
			msg <- HandlerCall{
				Ctx:     ctx,
				Message: message,
			}
			return <-resChan
		})

		if !sub.IsRunning() {
			sub.Start(context.Background())
		}
	}

	message := <-msg

	return message.Ctx, message.Message, resChan
}
