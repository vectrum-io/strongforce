// Package bustest builds bus messages for handler tests.
package bustest

import (
	"context"
	"time"

	"github.com/vectrum-io/strongforce/pkg/bus"
)

// Message returns a first delivery of a message with the given id and subject
// whose ack, nak and term do nothing. It is meant for calling Handle handlers
// directly, which receive their decoded event separately; its payload cannot
// be unmarshalled.
func Message(id string, subject string) bus.InboundMessage {
	return bus.InboundMessage{
		MessageCtx: context.Background(),
		Id:         id,
		Subject:    subject,
		Delivery:   bus.DeliveryInfo{NumDelivered: 1},
		Ack:        func() error { return nil },
		Nak:        func(time.Duration) error { return nil },
		Term:       func() error { return nil },
	}
}
