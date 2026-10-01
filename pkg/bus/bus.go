package bus

import (
	"context"
	"errors"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"time"
)

type Bus interface {
	// Publish sends a new message to the bus. It must be added in order to ensure consistency
	Publish(ctx context.Context, message *OutboundMessage) error
	// Subscribe retrieves messages from the bus in an ordered matter.
	Subscribe(ctx context.Context, subscriberName string, stream string, opts ...SubscribeOption) (*Subscription, error)
	// Migrate ensures that dependencies (streams, topic, consumers, etc.) are up to date and ready to be used
	Migrate(ctx context.Context) error
	// SubscriberInfo retrieves information about a subscriber in a stream
	SubscriberInfo(ctx context.Context, stream string, subscriberName string) (SubscriberInfo, error)
}

type SubscriberInfo interface {
	HasPendingMessages() bool
}

type OutboundMessage struct {
	Id      string
	Subject string
	Data    []byte
}

type InboundMessage struct {
	MessageCtx context.Context
	Id         string
	Subject    string
	Data       []byte
	Headers    map[string][]string
	Delivery   DeliveryInfo
	Ack        func() error
	Nak        func(retryAfter time.Duration) error
	// Term stops all further redeliveries of the message.
	Term         func() error
	deserializer serialization.Serializer
}

// DeliveryInfo is the broker-side position of a message. It is zero for
// messages that have no delivery tracking, e.g. core NATS broadcasts.
type DeliveryInfo struct {
	Stream         string
	Consumer       string
	StreamSequence uint64
	// NumDelivered counts deliveries of this message, starting at 1.
	NumDelivered uint64
}

// Unmarshal deserializes the message payload. Failures are Permanent: a payload
// that cannot be decoded will not decode on a retry either.
func (im *InboundMessage) Unmarshal(dst interface{}) error {
	if im.deserializer == nil {
		return Permanent(errors.New("message has no deserializer"))
	}
	if err := im.deserializer.Deserialize(im.Data, dst); err != nil {
		return Permanent(err)
	}
	return nil
}
