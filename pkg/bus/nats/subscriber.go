package nats

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"time"
)

type Subscriber struct {
	jetStream  jetstream.JetStream
	consumeCtx jetstream.ConsumeContext
}

type SubscriberOptions struct {
	NATSAddress string
}

type SubscribeOpts struct {
	CreateConsumer  bool
	ConsumerName    string
	DurableName     string
	DeliverPolicy   *jetstream.DeliverPolicy
	FilterSubject   string
	MaxAckPending   int
	MaxDeliverTries int
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
		jetStream: js,
	}, nil
}

func (ns *Subscriber) Subscribe(ctx context.Context, streamName string, opts *SubscribeOpts) (<-chan bus.InboundMessage, error) {
	msgChan := make(chan bus.InboundMessage)

	if opts == nil {
		opts = &SubscribeOpts{}
	}

	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

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
		ns.handleMessage(msg, msgChan)
	})
	if err != nil {
		return nil, err
	}

	ns.consumeCtx = consumeCtx

	return msgChan, nil
}

func (ns *Subscriber) handleMessage(msg jetstream.Msg, msgChan chan bus.InboundMessage) {
	msgChan <- bus.InboundMessage{
		Id:      msg.Headers().Get(jetstream.MsgIDHeader),
		Subject: msg.Subject(),
		Data:    msg.Data(),
		Ack: func() error {
			return msg.Ack()
		},
		Nak: func(delay time.Duration) error {
			return msg.NakWithDelay(delay)
		},
	}
}
