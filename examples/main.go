package main

import (
	"context"
	"fmt"
	nats2 "github.com/nats-io/nats.go"
	"github.com/vectrum-io/strongforce"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	"go.uber.org/zap"
	"time"
)

func main() {
	if err := SimpleNATSPubSub(); err != nil {
		panic(err)
	}
}

func SimpleNATSPubSub() error {
	logger, _ := zap.NewDevelopment()

	sf, err := strongforce.New(strongforce.WithNATS(&nats.Options{
		NATSAddress: "nats://localhost:65002",
		Logger:      logger,
		Streams: []nats2.StreamConfig{
			{
				Name:        "test",
				Description: "test stream",
				Subjects: []string{
					"test.>",
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
	}))

	if err != nil {
		return fmt.Errorf("failed to create strongforce client: %w", err)
	}

	if err := sf.Bus().Migrate(context.Background()); err != nil {
		return fmt.Errorf("failed to migrate bus: %w", err)
	}

	subscription, subscribeErr := sf.Bus().Subscribe(context.Background(), "test", "test")

	if subscribeErr != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", subscribeErr)
	}

	if err := subscription.AddHandler("test.>", func(ctx context.Context, message bus.InboundMessage) error {
		fmt.Printf("RECEIVED FROM NATS: %s\n", string(message.Data))
		return nil
	}); err != nil {
		return fmt.Errorf("failed to add handler: %w", err)
	}

	subscription.Start(context.Background())

	if err := sf.Bus().Publish(context.Background(), &bus.OutboundMessage{
		Id:      "1234",
		Subject: "test.hi",
		Data:    []byte("hello world"),
	}); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	time.Sleep(1 * time.Second)

	return nil
}
