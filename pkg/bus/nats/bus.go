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
	"time"
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
	// DeadLetter configures the shared dead-letter stream created by Migrate.
	DeadLetter DeadLetterOptions
	// Metrics is optional; nil disables subscription metrics.
	Metrics *bus.Metrics
	// Middleware wraps the handlers of every subscription, the first one
	// outermost.
	Middleware []bus.Middleware
}

// defaultAckWait is set on consumers that do not configure AckWait, so the
// handler timeout is derived from the AckWait the consumer really has.
const defaultAckWait = 30 * time.Second

// handlerTimeout leaves a tenth of AckWait to settle the message, so the
// broker does not redeliver it while its handlers are still running.
func handlerTimeout(ackWait time.Duration) time.Duration {
	return ackWait - ackWait/10
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

	filterSubjects := subscriptionOptions.FilterSubjects
	if len(subscriptionOptions.Routes) > 0 {
		if len(subscriptionOptions.FilterSubjects) > 0 {
			return nil, fmt.Errorf("%w: filter subjects are derived from Handle, do not combine it with WithFilterSubject", bus.ErrInvalidRoutes)
		}
		patterns, err := bus.ValidateRoutes(subscriptionOptions.Routes)
		if err != nil {
			return nil, err
		}
		filterSubjects = patterns
	}

	var deliverPolicy jetstream.DeliverPolicy

	switch subscriptionOptions.DeliveryPolicy {
	case bus.DeliverAll:
		deliverPolicy = jetstream.DeliverAllPolicy
	case bus.DeliverNew:
		deliverPolicy = jetstream.DeliverNewPolicy
	}

	// Concurrency drives two things in lockstep: how many goroutines the
	// subscription spawns and the JetStream MaxAckPending. Pinning them 1:1
	// means each in-flight message has an awake worker (or is moments away
	// from one) — its AckWait can't expire while the message sits in a queue.
	// GuaranteeOrder always wins: ordered delivery requires a single worker.
	concurrency := subscriptionOptions.Concurrency
	if concurrency <= 0 {
		concurrency = bus.DefaultConcurrency
	}
	if subscriptionOptions.GuaranteeOrder {
		concurrency = 1
	}

	ackWait := subscriptionOptions.AckWait
	if ackWait <= 0 {
		ackWait = defaultAckWait
	}

	var durableName string
	if subscriptionOptions.Durable {
		durableName = subscriberName
	}

	var deadLetter bus.DeadLetterFunc
	if !subscriptionOptions.DropOnExhaustion {
		// Without the stream every exhausted message would be retried
		// forever, so refuse to subscribe instead.
		if _, err := b.subscriber.jetStream.Stream(ctx, DeadLetterStreamName); err != nil {
			return nil, fmt.Errorf("dead-letter stream %q is not available (run Migrate or subscribe WithDropOnExhaustion): %w", DeadLetterStreamName, err)
		}
		deadLetter = b.broadcaster.PublishDeadLetter
	}

	// The attempt limit is enforced by the subscription, not the server:
	// a server-side MaxDeliver would drop messages whose handler kept
	// exceeding AckWait without them ever being dead-lettered.
	subscription, err := b.subscriber.Subscribe(ctx, stream, &SubscribeOpts{
		ConsumerName:    subscriberName,
		DurableName:     durableName,
		CreateConsumer:  true,
		DeliverPolicy:   &deliverPolicy,
		FilterSubjects:  filterSubjects,
		MaxDeliverTries: -1,
		MaxAckPending:   concurrency,
		Concurrency:     concurrency,
		AckWait:         ackWait,
		Deserializer:    subscriptionOptions.Deserializer,
		RetryPolicy:     subscriptionOptions.EffectiveRetryPolicy(),
		DeadLetter:      deadLetter,
		Metrics:         b.options.Metrics,
		Logger:          b.options.Logger,
		Routes:          subscriptionOptions.Routes,
		Middleware:      b.options.Middleware,
		HandlerTimeout:  handlerTimeout(ackWait),
	})
	if err != nil {
		return nil, err
	}

	return subscription, nil
}

func (b *Bus) Migrate(ctx context.Context) error {
	for _, streamConfig := range b.options.Streams {
		if streamConfig.Name == DeadLetterStreamName {
			return fmt.Errorf("stream name %q is reserved for the dead-letter stream, configure it with Options.DeadLetter", DeadLetterStreamName)
		}
	}

	conn, err := nats.Connect(b.options.NATSAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get jetstream context: %w", err)
	}

	streams := append([]nats.StreamConfig{b.options.DeadLetter.streamConfig(nil)}, b.options.Streams...)

	for _, streamConfig := range streams {
		b.logger.Infof("validating nats stream %s", streamConfig.Name)
		info, err := js.StreamInfo(streamConfig.Name)
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

		// The dead-letter stream is shared by every service; one must not
		// reset limits it does not configure itself.
		if streamConfig.Name == DeadLetterStreamName {
			streamConfig = b.options.DeadLetter.streamConfig(&info.Config)
		}

		b.logger.Infof("updating existing stream %s", streamConfig.Name)
		if _, err := js.UpdateStream(&streamConfig); err != nil {
			return fmt.Errorf("failed to update stream: %w", err)
		}
	}

	return nil
}

func (b *Bus) SubscriberInfo(ctx context.Context, stream string, consumerName string) (bus.SubscriberInfo, error) {
	consumer, err := b.subscriber.jetStream.Consumer(ctx, stream, consumerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}

	consumerInfo, err := consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer info: %w", err)
	}

	return &SubscriberInfo{
		jsConsumerInfo: consumerInfo,
	}, nil
}
