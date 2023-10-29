package events

import (
	"time"
)

type Builder struct {
}

func (b *Builder) New(topic string, payload interface{}) (*EventSpec, error) {
	return &EventSpec{
		Metadata: &EventMetadata{
			Id:        NewEventID(),
			Topic:     topic,
			CreatedAt: time.Now(),
		},
		Payload: payload,
	}, nil
}
