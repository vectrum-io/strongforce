package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	nats2 "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	sharedtest "github.com/vectrum-io/strongforce/tests/shared"
)

type handleTestEvent struct {
	Name string `json:"name"`
}

func TestBusHandleDeliversOnlyDeclaredSubjects(t *testing.T) {
	streamName := "test-handle"
	consumerName := fmt.Sprintf("%s-%d", streamName, time.Now().UnixNano())
	assert.NoError(t, sharedtest.CreateNatsStream(sharedtest.NATS, streamName, "test-handle.>"))
	natsBus := newMigratedBus(t)

	received := make(chan handleTestEvent, 10)
	subscription, err := natsBus.Subscribe(context.Background(), consumerName, streamName,
		bus.WithDeserializer(serialization.NewJSONSerializer()),
		bus.Handle("test-handle.wanted", func(ctx context.Context, event *handleTestEvent, message bus.InboundMessage) error {
			received <- *event
			return nil
		}),
	)
	assert.NoError(t, err)

	for _, subject := range []string{"test-handle.unrelated", "test-handle.wanted"} {
		assert.NoError(t, natsBus.Publish(context.Background(), &bus.OutboundMessage{
			Id:      consumerName + "-" + subject,
			Subject: subject,
			Data:    []byte(fmt.Sprintf(`{"name":%q}`, subject)),
		}))
	}
	subscription.Start(context.Background())

	select {
	case event := <-received:
		assert.Equal(t, "test-handle.wanted", event.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("declared subject was not delivered")
	}

	nc, err := nats2.Connect(sharedtest.NATS)
	assert.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	assert.NoError(t, err)

	info, err := js.ConsumerInfo(streamName, consumerName)
	if assert.NoError(t, err) {
		// NATS < 2.10 takes a single filter subject, newer servers a list.
		filters := append([]string{}, info.Config.FilterSubjects...)
		if info.Config.FilterSubject != "" {
			filters = append(filters, info.Config.FilterSubject)
		}
		assert.Equal(t, []string{"test-handle.wanted"}, filters)
		assert.Equal(t, 30*time.Second, info.Config.AckWait)
		assert.Zero(t, info.NumPending)
	}
	assert.Empty(t, received)
}

func TestBusSubscribeRejectsHandleCombinedWithFilterSubject(t *testing.T) {
	natsBus, err := nats.New(&nats.Options{NATSAddress: sharedtest.NATS})
	assert.NoError(t, err)

	_, err = natsBus.Subscribe(context.Background(), "test-handle-mixed", "test-handle",
		//lint:ignore SA1019 combining the deprecated filter option with Handle is what is rejected
		bus.WithFilterSubject("test-handle.other"),
		bus.HandleRaw("test-handle.wanted", func(ctx context.Context, message bus.InboundMessage) error {
			return nil
		}),
	)

	assert.ErrorIs(t, err, bus.ErrInvalidRoutes)
}
