package tests

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
	"reflect"
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

	messages, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName, bus.WithFilterSubject(subject))
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

	// validate order
	for i := 0; i < 5000; i++ {
		message := <-messages
		message.Ack()
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

	messagesA, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName, bus.WithFilterSubject(subject), bus.WithGuaranteeOrder())
	assert.NoError(t, err)

	messagesB, err := natsBus.Subscribe(context.Background(), streamName+"-"+subject, streamName, bus.WithFilterSubject(subject), bus.WithGuaranteeOrder())
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
	message1 := waitForMessage(messagesA, messagesB)
	assert.Equal(t, "0", message1.Id)
	t.Log("nak message 1")
	message1.Nak(0)

	// expect second message to still be message 0
	t.Log("wait for message 1 retry")
	message1Retry := waitForMessage(messagesA, messagesB)
	assert.Equal(t, "0", message1Retry.Id)
	t.Log("ack message 1 retry")
	message1Retry.Ack()

	t.Log("wait for message 2")
	message2 := waitForMessage(messagesA, messagesB)
	message2.Ack()
	t.Log("ack message 2")
	assert.Equal(t, "1", message2.Id)

}

func waitForMessage(channels ...<-chan bus.InboundMessage) bus.InboundMessage {
	cases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	_, msg, _ := reflect.Select(cases)

	return msg.Interface().(bus.InboundMessage)
}
