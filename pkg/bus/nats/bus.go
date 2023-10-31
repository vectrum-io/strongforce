package nats

import (
	"context"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"go.uber.org/zap"
)

type Bus struct {
	subscriber  *Subscriber
	broadcaster *Broadcaster
}

type Options struct {
	NATSAddress string
	Logger      *zap.Logger
}

func New(options *Options) (*Bus, error) {

	if options.Logger == nil {
		options.Logger = zap.L()
	}

	subscriber, err := NewSubscriber(&SubscriberOptions{
		NATSAddress: options.NATSAddress,
	})
	if err != nil {
		return nil, err
	}

	broadcaster, err := NewBroadcaster(&BroadcasterOptions{
		NATSAddress: options.NATSAddress,
		Logger:      options.Logger.Sugar(),
	})
	if err != nil {
		return nil, err
	}

	return &Bus{
		subscriber:  subscriber,
		broadcaster: broadcaster,
	}, nil
}

func (b *Bus) Publish(ctx context.Context, message *bus.OutboundMessage) error {
	return b.broadcaster.Broadcast(ctx, message)
}

func (b *Bus) Subscribe(ctx context.Context, subscriberName string, stream string, opts ...bus.SubscribeOption) (<-chan bus.InboundMessage, error) {
	subscriptionOptions := bus.DefaultSubscriptionOptions
	for _, opt := range opts {
		opt(&subscriptionOptions)
	}

	var deliverPolicy jetstream.DeliverPolicy

	switch subscriptionOptions.DeliveryPolicy {
	case bus.DeliverNew:
		deliverPolicy = jetstream.DeliverNewPolicy
	case bus.DeliverAll:
		deliverPolicy = jetstream.DeliverAllPolicy
	}

	maxAckPending := -1
	if subscriptionOptions.GuaranteeOrder {
		maxAckPending = 1
	}

	eventsChan, err := b.subscriber.Subscribe(ctx, stream, &SubscribeOpts{
		ConsumerName:    subscriberName,
		CreateConsumer:  true,
		DeliverPolicy:   &deliverPolicy,
		FilterSubject:   subscriptionOptions.FilterSubject,
		MaxDeliverTries: subscriptionOptions.MaxDeliveryTries,
		MaxAckPending:   maxAckPending,
	})
	if err != nil {
		return nil, err
	}

	return eventsChan, nil
}
