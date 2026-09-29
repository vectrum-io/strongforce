package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"time"
)

// defaultPublishWait matches the JetStream context's default publish wait.
const defaultPublishWait = 5 * time.Second

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

	// A publish with a context waits until the context ends, so one without a
	// deadline gets the wait a publish without a context would have.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPublishWait)
		defer cancel()
	}

	_, err := nb.jetStream.PublishMsg(&nats.Msg{
		Header:  headers,
		Subject: message.Subject,
		Data:    message.Data,
	}, nats.MsgId(message.Id), nats.Context(ctx))

	return err
}
