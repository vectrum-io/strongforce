package tests

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	"testing"
)

func TestBusOrderSpam(t *testing.T) {
	streamName := "test-spam"
	subject := "test-1"

	// create test stream
	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: "127.0.0.1:65002",
	})
	assert.NoError(t, err)

	subscription, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName, bus.WithFilterSubject(subject))
	assert.NoError(t, err)

	// send 5k messages
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

	// validate order
	for i := 0; i < 5000; i++ {
		message := <-msgChan
		assert.Equal(t, fmt.Sprintf("%d", i), string(message.Data))
	}
}

func TestBusOrderConsumerNak(t *testing.T) {
	streamName := "test-order"
	subject := "test-2"

	// create test stream
	err := sharedtest.CreateNatsStream(sharedtest.NATS, streamName, subject)
	assert.NoError(t, err)

	natsBus, err := nats.New(&nats.Options{
		NATSAddress: "127.0.0.1:65002",
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
	message1, res := waitForMessage(subscriptionA, subscriptionB)
	assert.Equal(t, "0", message1.Id)
	t.Log("nak message 1")
	message1.Nak(0)
	res <- errors.New("failed")

	// expect second message to still be message 0
	t.Log("wait for message 1 retry")
	message1Retry, res := waitForMessage(subscriptionA, subscriptionB)
	assert.Equal(t, "0", message1Retry.Id)
	res <- nil

	t.Log("wait for message 2")
	message2, res := waitForMessage(subscriptionA, subscriptionB)
	res <- nil
	assert.Equal(t, "1", message2.Id)

}

func waitForMessage(subscriptions ...*bus.Subscription) (bus.InboundMessage, chan error) {
	resChan := make(chan error)
	msg := make(chan bus.InboundMessage)

	for _, sub := range subscriptions {

		if sub.IsRunning() {
			sub.RemoveHandler(">")
		}

		sub.AddHandler(">", func(ctx context.Context, message bus.InboundMessage) error {
			msg <- message
			return <-resChan
		})

		if !sub.IsRunning() {
			sub.Start(context.Background())
		}
	}

	message := <-msg

	return message, resChan
}
