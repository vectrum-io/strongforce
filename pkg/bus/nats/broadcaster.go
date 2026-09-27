package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

type Broadcaster struct {
	jetStream      nats.JetStreamContext
	logger         *zap.SugaredLogger
	otelPropagator propagation.TextMapPropagator
}

type BroadcasterOptions struct {
	NATSAddress    string
	Logger         *zap.SugaredLogger
	OTelPropagator propagation.TextMapPropagator
}

func NewBroadcaster(opts *BroadcasterOptions) (*Broadcaster, error) {
	nc, err := nats.Connect(opts.NATSAddress)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, err
	}

	return &Broadcaster{
		jetStream:      js,
		logger:         opts.Logger,
		otelPropagator: opts.OTelPropagator,
	}, nil
}

func (nb *Broadcaster) Broadcast(ctx context.Context, message *bus.OutboundMessage) error {
	nb.logger.Debugf("Broadcasting event to %+v", message.Subject)

	headers := nats.Header{}

	// inject otel metadata into nats message headers
	if nb.otelPropagator != nil {
		nb.otelPropagator.Inject(ctx, propagation.HeaderCarrier(headers))
	}

	opts := []nats.PubOpt{nats.MsgId(message.Id)}
	// JetStream only accepts a publish context that carries a deadline;
	// without one the connection's default wait applies.
	if _, ok := ctx.Deadline(); ok {
		opts = append(opts, nats.Context(ctx))
	}

	_, err := nb.jetStream.PublishMsg(&nats.Msg{
		Header:  headers,
		Subject: message.Subject,
		Data:    message.Data,
	}, opts...)

	return err
}
