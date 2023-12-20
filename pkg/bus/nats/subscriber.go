package nats

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.opentelemetry.io/otel/propagation"
	"time"
)

type Subscriber struct {
	jetStream      jetstream.JetStream
	conn           *nats.Conn
	otelPropagator propagation.TextMapPropagator
}

type SubscriberOptions struct {
	NATSAddress    string
	OTelPropagator propagation.TextMapPropagator
}

type SubscribeOpts struct {
	CreateConsumer  bool
	ConsumerName    string
	DurableName     string
	DeliverPolicy   *jetstream.DeliverPolicy
	FilterSubject   string
	MaxAckPending   int
	MaxDeliverTries int
	MessageBuffer   int
	Deserializer    serialization.Serializer
}

func (so *SubscribeOpts) validate() error {
	if so.MaxAckPending == 0 {
		so.MaxAckPending = -1
	}

	if so.MaxDeliverTries == 0 {
		so.MaxDeliverTries = 10
	}

	if so.DeliverPolicy == nil {
		last := jetstream.DeliverLastPolicy
		so.DeliverPolicy = &last
	}

	if so.MessageBuffer == 0 {
		so.MessageBuffer = 256
	}

	if so.Deserializer == nil {
		so.Deserializer = serialization.NewProtobufSerializer()
	}

	return nil
}

type SubscribeBroadcastOpts struct {
	MessageBuffer int
	Deserializer  serialization.Serializer
}

func (so *SubscribeBroadcastOpts) validate() error {
	if so.MessageBuffer == 0 {
		so.MessageBuffer = 256
	}

	if so.Deserializer == nil {
		so.Deserializer = serialization.NewProtobufSerializer()
	}

	return nil
}

func NewSubscriber(opts *SubscriberOptions) (*Subscriber, error) {
	nc, err := nats.Connect(
		opts.NATSAddress,
	)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		jetStream:      js,
		conn:           nc,
		otelPropagator: opts.OTelPropagator,
	}, nil
}

func (ns *Subscriber) SubscribeBroadcast(ctx context.Context, subject string, opts *SubscribeBroadcastOpts) (*bus.Subscription, error) {
	if opts == nil {
		opts = &SubscribeBroadcastOpts{}
	}

	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

	msgChan := make(chan bus.InboundMessage, opts.MessageBuffer)

	subscription, err := ns.conn.Subscribe(subject, func(msg *nats.Msg) {
		ns.handleNATSMessage(msg, msgChan)
	})
	if err != nil {
		return nil, err
	}

	return bus.NewSubscription(msgChan, opts.Deserializer, func() {
		_ = subscription.Drain()
		_ = subscription.Unsubscribe()
	}), nil
}

func (ns *Subscriber) Subscribe(ctx context.Context, streamName string, opts *SubscribeOpts) (*bus.Subscription, error) {
	if opts == nil {
		opts = &SubscribeOpts{}
	}

	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

	msgChan := make(chan bus.InboundMessage, opts.MessageBuffer)

	stream, err := ns.jetStream.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("stream not found at jetstream: %w", err)
	}

	var consumer jetstream.Consumer
	if !opts.CreateConsumer {
		existingConsumer, err := stream.Consumer(ctx, opts.ConsumerName)
		if err != nil {
			return nil, err
		}
		consumer = existingConsumer
	} else {
		newConsumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:          opts.ConsumerName,
			Durable:       opts.DurableName,
			DeliverPolicy: *opts.DeliverPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: opts.FilterSubject,
			MaxDeliver:    opts.MaxDeliverTries,
			MaxAckPending: opts.MaxAckPending,
		})
		if err != nil {
			return nil, err
		}
		consumer = newConsumer
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		ns.handleJetStreamMessage(msg, msgChan)
	})
	if err != nil {
		return nil, err
	}

	return bus.NewSubscription(msgChan, opts.Deserializer, func() {
		consumeCtx.Stop()
	}), nil
}

func (ns *Subscriber) handleNATSMessage(msg *nats.Msg, msgChan chan bus.InboundMessage) {
	msgChan <- bus.InboundMessage{
		MessageCtx: ns.getMessageCtx(context.Background(), msg.Header),
		Id:         msg.Header.Get(nats.MsgIdHdr),
		Subject:    msg.Subject,
		Data:       msg.Data,
		Ack: func() error {
			return msg.Ack()
		},
		Nak: func(delay time.Duration) error {
			return msg.NakWithDelay(delay)
		},
	}
}

func (ns *Subscriber) handleJetStreamMessage(msg jetstream.Msg, msgChan chan bus.InboundMessage) {
	msgChan <- bus.InboundMessage{
		MessageCtx: ns.getMessageCtx(context.Background(), msg.Headers()),
		Id:         msg.Headers().Get(jetstream.MsgIDHeader),
		Subject:    msg.Subject(),
		Data:       msg.Data(),
		Ack: func() error {
			return msg.Ack()
		},
		Nak: func(delay time.Duration) error {
			return msg.NakWithDelay(delay)
		},
	}
}

func (ns *Subscriber) getMessageCtx(ctx context.Context, header nats.Header) context.Context {
	if ns.otelPropagator == nil {
		return ctx
	}
	return ns.otelPropagator.Extract(ctx, propagation.HeaderCarrier(header))
}
