package outbox

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

type Outbox struct {
	tableName  string
	serializer serialization.Serializer
}

func New(options *Options) (*Outbox, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

	return &Outbox{
		tableName:  options.TableName,
		serializer: options.Serializer,
	}, nil
}

func (o *Outbox) EmitEvent(ctx context.Context, tx *sqlx.Tx, event *events.EventSpec) (*events.EventID, error) {
	serializedEvent, err := o.serializer.Serialize(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize event: %w", err)
	}

	metadata := event.Metadata

	query := tx.Rebind(fmt.Sprintf(`
		INSERT INTO %s (id, topic, payload) VALUES (?, ?, ?)
	`, o.tableName))

	_, err = tx.ExecContext(ctx, query, metadata.Id.String(), metadata.Topic, serializedEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to store event to db: %w", err)
	}

	return &metadata.Id, nil
}
