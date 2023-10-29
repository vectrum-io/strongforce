package events

import (
	"time"
)

type EventSpec struct {
	Metadata *EventMetadata
	Payload  interface{}
}

type SerializedEvent struct {
	Metadata          *EventMetadata
	SerializedPayload []byte
}

type EventMetadata struct {
	Id        EventID
	Topic     string
	CreatedAt time.Time
}
