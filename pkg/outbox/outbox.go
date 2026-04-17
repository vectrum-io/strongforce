package outbox

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/serialization"
)

type Outbox struct {
	tableName  string
	serializer serialization.Serializer
	notifier   atomic.Pointer[CommitNotifier]
}

func New(options *Options) (*Outbox, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

	ob := &Outbox{
		tableName:  options.TableName,
		serializer: options.Serializer,
	}
	if options.Notifier != nil {
		ob.SetNotifier(options.Notifier)
	}
	return ob, nil
}

func (o *Outbox) TableName() string {
	return o.tableName
}

// SetNotifier attaches a CommitNotifier. Safe to call at any time; replaces
// any previously set notifier. Pass nil to detach.
func (o *Outbox) SetNotifier(n CommitNotifier) {
	if n == nil {
		o.notifier.Store(nil)
		return
	}
	o.notifier.Store(&n)
}

// NotifyCommitted is called by the db layer after a successful tx.Commit().
// It is a no-op when no notifier is registered.
func (o *Outbox) NotifyCommitted(ctx context.Context, evs []*events.SerializedEvent) {
	if len(evs) == 0 {
		return
	}
	np := o.notifier.Load()
	if np == nil {
		return
	}
	(*np).NotifyCommitted(ctx, evs)
}

// EmitEvent serializes and inserts the event into the outbox table within the
// provided transaction. It returns the SerializedEvent so callers can hand the
// exact persisted bytes to a CommitNotifier without re-serializing.
func (o *Outbox) EmitEvent(ctx context.Context, tx *sqlx.Tx, event *events.EventSpec) (*events.SerializedEvent, error) {
	serializedPayload, err := o.serializer.Serialize(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize event: %w", err)
	}

	metadata := event.Metadata

	query := tx.Rebind(fmt.Sprintf(`
		INSERT INTO %s (id, topic, payload) VALUES (?, ?, ?)
	`, o.tableName))

	if _, err := tx.ExecContext(ctx, query, metadata.Id.String(), metadata.Topic, serializedPayload); err != nil {
		return nil, fmt.Errorf("failed to store event to db: %w", err)
	}

	return &events.SerializedEvent{
		Metadata:          metadata,
		SerializedPayload: serializedPayload,
	}, nil
}
