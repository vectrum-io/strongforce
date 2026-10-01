//lint:file-ignore SA1019 these tests cover subscriptions that declare filter subjects and handlers separately

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
	"sync/atomic"
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

	natsBus := newMigratedBus(t)

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

	natsBus := newMigratedBus(t)

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

	natsBus := newMigratedBus(t)

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

	natsBus := newMigratedBus(t)

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
	assert.Equal(t, "test-val", message.MessageCtx.Value(testCtxKey{}))
	assert.Equal(t, "test-val", ctx.Value(testCtxKey{}))
	_, hasDeadline := ctx.Deadline()
	assert.True(t, hasDeadline, "handlers run with a deadline below AckWait")
	res <- nil
}

func TestSubscribeContextCancelation(t *testing.T) {
	streamName := "test-context-cancel"
	subject := "test-4"

	// create test stream
	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus := newMigratedBus(t)

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

var fastRetryPolicy = bus.RetryPolicy{
	MaxAttempts:  3,
	InitialDelay: 10 * time.Millisecond,
	MaxDelay:     20 * time.Millisecond,
	Multiplier:   2,
}

func newMigratedBus(t *testing.T) *nats.Bus {
	t.Helper()

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: sharedtest.NATS,
	})
	assert.NoError(t, err)
	assert.NoError(t, natsBus.Migrate(context.Background()))

	return natsBus
}

func purgeDeadLetters(t *testing.T, subject string) nats2.JetStreamContext {
	t.Helper()

	nc, err := nats2.Connect(sharedtest.NATS)
	assert.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	assert.NoError(t, err)
	assert.NoError(t, js.PurgeStream(nats.DeadLetterStreamName, &nats2.StreamPurgeRequest{Subject: subject}))

	return js
}

func publishNumbered(t *testing.T, natsBus *nats.Bus, subject string, ids ...string) {
	t.Helper()

	for _, id := range ids {
		assert.NoError(t, natsBus.Publish(context.Background(), &bus.OutboundMessage{
			Id:      subject + "-" + id,
			Subject: subject,
			Data:    []byte(id),
		}))
	}
}

func TestBusDeadLettersExhaustedMessageAndContinuesInOrder(t *testing.T) {
	streamName := "test-dead-letter"
	subject := "test-dead-letter-subject"
	// Unique per run: dead letters are deduplicated by stream sequence, which
	// restarts at 1 whenever the test recreates the stream.
	consumerName := fmt.Sprintf("%s-%d", streamName, time.Now().UnixNano())

	assert.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject))
	natsBus := newMigratedBus(t)
	dlqSubject := nats.DeadLetterSubject(streamName, consumerName)
	js := purgeDeadLetters(t, dlqSubject)

	subscription, err := natsBus.Subscribe(context.Background(), consumerName, streamName,
		bus.WithFilterSubject(subject), bus.WithGuaranteeOrder(), bus.WithRetryPolicy(fastRetryPolicy))
	assert.NoError(t, err)

	var attempts atomic.Int32
	received := make(chan string, 10)
	assert.NoError(t, subscription.AddHandler(subject, func(ctx context.Context, message bus.InboundMessage) error {
		if string(message.Data) == "poison" {
			attempts.Add(1)
			return errors.New("downstream unavailable")
		}
		received <- string(message.Data)
		return nil
	}))

	publishNumbered(t, natsBus, subject, "poison", "next")
	subscription.Start(context.Background())

	select {
	case data := <-received:
		assert.Equal(t, "next", data)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not move past the exhausted message")
	}
	assert.Equal(t, int32(3), attempts.Load())

	deadLetter, err := js.GetLastMsg(nats.DeadLetterStreamName, dlqSubject)
	if assert.NoError(t, err) {
		assert.Equal(t, "poison", string(deadLetter.Data))
		assert.Equal(t, subject, deadLetter.Header.Get(nats.DeadLetterHeaderSubject))
		assert.Equal(t, streamName, deadLetter.Header.Get(nats.DeadLetterHeaderStream))
		assert.Equal(t, consumerName, deadLetter.Header.Get(nats.DeadLetterHeaderConsumer))
		assert.Equal(t, "3", deadLetter.Header.Get(nats.DeadLetterHeaderNumDelivered))
		assert.Equal(t, "1", deadLetter.Header.Get(nats.DeadLetterHeaderStreamSequence))
		assert.Equal(t, subject+"-poison", deadLetter.Header.Get(nats.DeadLetterHeaderMessageId))
		assert.Contains(t, deadLetter.Header.Get(nats.DeadLetterHeaderError), "downstream unavailable")
	}
}

func TestBusDeadLettersPermanentErrorWithoutRetry(t *testing.T) {
	streamName := "test-dead-letter-permanent"
	subject := "test-dead-letter-permanent-subject"
	// Unique per run: dead letters are deduplicated by stream sequence, which
	// restarts at 1 whenever the test recreates the stream.
	consumerName := fmt.Sprintf("%s-%d", streamName, time.Now().UnixNano())

	assert.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject))
	natsBus := newMigratedBus(t)
	dlqSubject := nats.DeadLetterSubject(streamName, consumerName)
	js := purgeDeadLetters(t, dlqSubject)

	subscription, err := natsBus.Subscribe(context.Background(), consumerName, streamName,
		bus.WithFilterSubject(subject), bus.WithGuaranteeOrder(), bus.WithRetryPolicy(fastRetryPolicy))
	assert.NoError(t, err)

	var attempts atomic.Int32
	assert.NoError(t, subscription.AddHandler(subject, func(ctx context.Context, message bus.InboundMessage) error {
		attempts.Add(1)
		return bus.Permanent(errors.New("incident does not exist"))
	}))

	publishNumbered(t, natsBus, subject, "gone")
	subscription.Start(context.Background())

	assert.Eventually(t, func() bool {
		_, err := js.GetLastMsg(nats.DeadLetterStreamName, dlqSubject)
		return err == nil
	}, 10*time.Second, 20*time.Millisecond)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestBusDropsExhaustedMessageWhenConfigured(t *testing.T) {
	streamName := "test-drop-exhausted"
	subject := "test-drop-exhausted-subject"
	// Unique per run: dead letters are deduplicated by stream sequence, which
	// restarts at 1 whenever the test recreates the stream.
	consumerName := fmt.Sprintf("%s-%d", streamName, time.Now().UnixNano())

	assert.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject))
	natsBus := newMigratedBus(t)
	dlqSubject := nats.DeadLetterSubject(streamName, consumerName)
	js := purgeDeadLetters(t, dlqSubject)

	subscription, err := natsBus.Subscribe(context.Background(), consumerName, streamName,
		bus.WithFilterSubject(subject), bus.WithGuaranteeOrder(), bus.WithRetryPolicy(fastRetryPolicy),
		bus.WithDropOnExhaustion())
	assert.NoError(t, err)

	received := make(chan string, 10)
	assert.NoError(t, subscription.AddHandler(subject, func(ctx context.Context, message bus.InboundMessage) error {
		if string(message.Data) == "stale" {
			return errors.New("stale heartbeat")
		}
		received <- string(message.Data)
		return nil
	}))

	publishNumbered(t, natsBus, subject, "stale", "fresh")
	subscription.Start(context.Background())

	select {
	case data := <-received:
		assert.Equal(t, "fresh", data)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not move past the exhausted message")
	}

	_, err = js.GetLastMsg(nats.DeadLetterStreamName, dlqSubject)
	assert.ErrorIs(t, err, nats2.ErrMsgNotFound)
}

func TestBusDeadLettersUnroutableMessageAndContinuesInOrder(t *testing.T) {
	streamName := "test-unroutable"
	// Unique per run: dead letters are deduplicated by stream sequence, which
	// restarts at 1 whenever the test recreates the stream.
	consumerName := fmt.Sprintf("%s-%d", streamName, time.Now().UnixNano())

	assert.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, streamName, "unroutable.>"))
	natsBus := newMigratedBus(t)
	dlqSubject := nats.DeadLetterSubject(streamName, consumerName)
	js := purgeDeadLetters(t, dlqSubject)

	subscription, err := natsBus.Subscribe(context.Background(), consumerName, streamName,
		bus.WithFilterSubject("unroutable.>"), bus.WithGuaranteeOrder(), bus.WithRetryPolicy(fastRetryPolicy))
	assert.NoError(t, err)

	received := make(chan string, 10)
	assert.NoError(t, subscription.AddHandler("unroutable.handled", func(ctx context.Context, message bus.InboundMessage) error {
		received <- string(message.Data)
		return nil
	}))

	publishNumbered(t, natsBus, "unroutable.ignored", "ignored")
	publishNumbered(t, natsBus, "unroutable.handled", "handled")
	subscription.Start(context.Background())

	select {
	case data := <-received:
		assert.Equal(t, "handled", data)
	case <-time.After(10 * time.Second):
		t.Fatal("unroutable message blocked the ordered consumer")
	}

	deadLetter, err := js.GetLastMsg(nats.DeadLetterStreamName, dlqSubject)
	if assert.NoError(t, err) {
		assert.Equal(t, "ignored", string(deadLetter.Data))
		assert.Contains(t, deadLetter.Header.Get(nats.DeadLetterHeaderError), bus.ErrMessageNotRoutable.Error())
	}
}

func TestBusRetriesMessageThatArrivesBeforeItsHandler(t *testing.T) {
	streamName := "test-late-handler"
	subject := "test-late-handler-subject"
	consumerName := fmt.Sprintf("%s-%d", streamName, time.Now().UnixNano())

	assert.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject))
	natsBus := newMigratedBus(t)

	subscription, err := natsBus.Subscribe(context.Background(), consumerName, streamName,
		bus.WithFilterSubject(subject), bus.WithGuaranteeOrder(), bus.WithRetryPolicy(bus.RetryPolicy{
			MaxAttempts:  20,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     50 * time.Millisecond,
		}))
	assert.NoError(t, err)

	publishNumbered(t, natsBus, subject, "early")
	subscription.Start(context.Background())
	time.Sleep(200 * time.Millisecond)

	received := make(chan string, 1)
	assert.NoError(t, subscription.AddHandler(subject, func(ctx context.Context, message bus.InboundMessage) error {
		received <- string(message.Data)
		return nil
	}))

	select {
	case data := <-received:
		assert.Equal(t, "early", data)
	case <-time.After(5 * time.Second):
		t.Fatal("message that arrived before its handler was lost")
	}
}

func TestBusPublishObservesCancellationWithoutDeadline(t *testing.T) {
	streamName := "test-publish-cancel"
	subject := "test-publish-cancel-subject"
	assert.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject))

	natsBus, err := nats.New(&nats.Options{NATSAddress: sharedtest.NATS})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = natsBus.Publish(ctx, &bus.OutboundMessage{Id: "cancelled", Subject: subject, Data: []byte("x")})

	assert.ErrorIs(t, err, context.Canceled)
}
