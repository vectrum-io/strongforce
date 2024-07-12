package nats

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

type Bus struct {
	subscriber  *Subscriber
	broadcaster *Broadcaster
	options     *Options
	logger      *zap.SugaredLogger
}

type Options struct {
	NATSAddress    string
	Logger         *zap.Logger
	Streams        []nats.StreamConfig
	OTelPropagator propagation.TextMapPropagator
}

func New(options *Options) (*Bus, error) {

	if options.Logger == nil {
		options.Logger = zap.L()
	}

	subscriber, err := NewSubscriber(&SubscriberOptions{
		NATSAddress:    options.NATSAddress,
		OTelPropagator: options.OTelPropagator,
	})
	if err != nil {
		return nil, err
	}

	broadcaster, err := NewBroadcaster(&BroadcasterOptions{
		NATSAddress:    options.NATSAddress,
		Logger:         options.Logger.Sugar(),
		OTelPropagator: options.OTelPropagator,
	})
	if err != nil {
		return nil, err
	}

	return &Bus{
		subscriber:  subscriber,
		broadcaster: broadcaster,
		options:     options,
		logger:      options.Logger.Sugar(),
	}, nil
}

func (b *Bus) Publish(ctx context.Context, message *bus.OutboundMessage) error {
	return b.broadcaster.Broadcast(ctx, message)
}

func (b *Bus) Subscribe(ctx context.Context, subscriberName string, stream string, opts ...bus.SubscribeOption) (*bus.Subscription, error) {
	subscriptionOptions := bus.DefaultSubscriptionOptions
	for _, opt := range opts {
		opt(&subscriptionOptions)
	}

	var deliverPolicy jetstream.DeliverPolicy

	switch subscriptionOptions.DeliveryPolicy {
	case bus.DeliverAll:
		deliverPolicy = jetstream.DeliverAllPolicy
	case bus.DeliverNew:
		deliverPolicy = jetstream.DeliverNewPolicy
	}

	maxAckPending := -1
	if subscriptionOptions.GuaranteeOrder {
		maxAckPending = 1
	}

	var durableName string
	if subscriptionOptions.Durable {
		durableName = subscriberName
	}

	subscription, err := b.subscriber.Subscribe(ctx, stream, &SubscribeOpts{
		ConsumerName:    subscriberName,
		DurableName:     durableName,
		CreateConsumer:  true,
		DeliverPolicy:   &deliverPolicy,
		FilterSubjects:  subscriptionOptions.FilterSubjects,
		MaxDeliverTries: subscriptionOptions.MaxDeliveryTries,
		MaxAckPending:   maxAckPending,
		Deserializer:    subscriptionOptions.Deserializer,
	})
	if err != nil {
		return nil, err
	}

	return subscription, nil
}

func (b *Bus) Migrate(ctx context.Context) error {
	conn, err := nats.Connect(b.options.NATSAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get jetstream context: %w", err)
	}

	for _, streamConfig := range b.options.Streams {
		b.logger.Infof("validating nats stream %s", streamConfig.Name)
		_, err := js.StreamInfo(streamConfig.Name)
		if err != nil {
			if !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to get stream info: %w", err)
			}

			// create new stream
			b.logger.Infof("creating new stream %s", streamConfig.Name)
			if _, err := js.AddStream(&streamConfig); err != nil {
				return fmt.Errorf("failed to add stream: %w", err)
			}
			continue
		}

		b.logger.Infof("updating existing stream %s", streamConfig.Name)
		if _, err := js.UpdateStream(&streamConfig); err != nil {
			return fmt.Errorf("failed to update stream: %w", err)
		}
	}

	return nil
}
