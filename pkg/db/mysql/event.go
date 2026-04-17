package mysql

import (
	"context"
	"errors"
	"fmt"

	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/events"
)

var (
	ErrNoOutboxConfigured = errors.New("no event outbox configured")
)

// EventTx is a convenience method for emitting a single event in a single transaction.
// On successful commit, the event is handed to the outbox's CommitNotifier (if any)
// so downstream consumers can publish it directly without re-querying the outbox.
func (db *MySQL) EventTx(ctx context.Context, etxFn db.EventTxFn) (eventId *events.EventID, err error) {
	if db.outbox == nil {
		return nil, ErrNoOutboxConfigured
	}

	tx, err := db.conn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic occurred: %+v", p)
			if rbError := tx.Rollback(); rbError != nil {
				err = errors.Join(err, rbError)
			}
		} else if err != nil {
			if rbError := tx.Rollback(); rbError != nil {
				err = errors.Join(err, rbError)
			}
		}
	}()

	event, txErr := etxFn(ctx, tx)
	if txErr != nil {
		err = txErr
		return nil, txErr
	}

	serialized, err := db.outbox.EmitEvent(ctx, tx, event)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	db.outbox.NotifyCommitted(ctx, []*events.SerializedEvent{serialized})

	id := serialized.Metadata.Id
	return &id, nil
}

// EventsTx is a convenience method for emitting multiple events in a single transaction.
// On successful commit, all events are handed to the outbox's CommitNotifier (if any).
func (db *MySQL) EventsTx(ctx context.Context, etxFn db.EventsTxFn) (eventIds []events.EventID, err error) {
	if db.outbox == nil {
		return nil, ErrNoOutboxConfigured
	}

	tx, err := db.conn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic occurred: %+v", p)
			if rbError := tx.Rollback(); rbError != nil {
				err = errors.Join(err, rbError)
			}
		} else if err != nil {
			if rbError := tx.Rollback(); rbError != nil {
				err = errors.Join(err, rbError)
			}
		}
	}()

	eventSpecs, txErr := etxFn(ctx, tx)
	if txErr != nil {
		err = txErr
		return nil, txErr
	}

	serializedEvents := make([]*events.SerializedEvent, 0, len(eventSpecs))
	for _, spec := range eventSpecs {
		serialized, emitErr := db.outbox.EmitEvent(ctx, tx, spec)
		if emitErr != nil {
			err = emitErr
			return nil, emitErr
		}
		serializedEvents = append(serializedEvents, serialized)
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	db.outbox.NotifyCommitted(ctx, serializedEvents)

	eventIds = make([]events.EventID, 0, len(serializedEvents))
	for _, s := range serializedEvents {
		eventIds = append(eventIds, s.Metadata.Id)
	}
	return eventIds, nil
}
