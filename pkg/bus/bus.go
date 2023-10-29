package bus

import (
	"context"
	"time"
)

type Bus interface {
	// Publish sends a new message to the bus. It must be added in order to ensure consistency
	Publish(ctx context.Context, message *OutboundMessage) error
	// Subscribe retrieves messages from the bus in an ordered matter.
	Subscribe(ctx context.Context, subscriberName string, stream string, opts ...SubscribeOption) (<-chan InboundMessage, error)
}

type OutboundMessage struct {
	Id      string
	Subject string
	Data    []byte
}

type InboundMessage struct {
	Id      string
	Subject string
	Data    []byte
	Ack     func() error
	Nak     func(duration time.Duration) error
}
