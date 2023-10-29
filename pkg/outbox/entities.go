package outbox

import (
	"database/sql"
	"github.com/pkg/errors"
	"github.com/vectrum-io/strongforce/pkg/events"
)

var (
	ErrEventIdInvalid = errors.New("event id invalid")
)

type EventEntity struct {
	Id        sql.NullString `db:"id"`
	Topic     sql.NullString `db:"topic"`
	Payload   []byte         `db:"payload"`
	CreatedAt sql.NullTime   `db:"created_at"`
}

func (ee *EventEntity) ToSerializedEvent() (*events.SerializedEvent, error) {
	if !ee.Id.Valid {
		return nil, ErrEventIdInvalid
	}

	return &events.SerializedEvent{
		Metadata: &events.EventMetadata{
			Id:        events.EventID(ee.Id.String),
			Topic:     ee.Topic.String,
			CreatedAt: ee.CreatedAt.Time,
		},
		SerializedPayload: ee.Payload,
	}, nil
}
