package mocks

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/vectrum-io/strongforce/pkg/bus"
)

type Bus struct {
	mock.Mock
}

func (b *Bus) Publish(ctx context.Context, message *bus.OutboundMessage) error {
	args := b.Called(*message)
	return args.Error(0)
}

func (b *Bus) Subscribe(ctx context.Context, subscribeName string, stream string, opts ...bus.SubscribeOption) (*bus.Subscription, error) {
	subscribeOpts := bus.DefaultSubscriptionOptions
	for _, opt := range opts {
		opt(&subscribeOpts)
	}

	args := b.Called(subscribeName, stream, subscribeOpts)
	return args.Get(0).(*bus.Subscription), args.Error(1)
}

func (b *Bus) Migrate(ctx context.Context) error {
	args := b.Called()
	return args.Error(0)
}

func (b *Bus) SubscriberInfo(ctx context.Context, stream string, consumerName string) (bus.SubscriberInfo, error) {
	args := b.Called(stream, consumerName)
	return args.Get(0).(bus.SubscriberInfo), args.Error(1)
}
