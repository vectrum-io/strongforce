package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/vectrum-io/strongforce/pkg/bus"
	"go.uber.org/zap"
)

type Broadcaster struct {
	jetStream nats.JetStreamContext
	logger    *zap.SugaredLogger
}

type BroadcasterOptions struct {
	NATSAddress string
	Logger      *zap.SugaredLogger
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
		jetStream: js,
		logger:    opts.Logger,
	}, nil
}

func (nb *Broadcaster) Broadcast(ctx context.Context, message *bus.OutboundMessage) error {
	nb.logger.Debugf("Broadcasting event to %+v", message.Subject)

	_, err := nb.jetStream.PublishMsg(&nats.Msg{
		Subject: message.Subject,
		Data:    message.Data,
	}, nats.MsgId(message.Id))

	return err
}
