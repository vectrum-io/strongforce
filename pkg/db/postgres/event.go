package postgres

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
func (db *PostgresSQL) EventTx(ctx context.Context, etxFn db.EventTxFn) (eventId *events.EventID, err error) {
	if db.outbox == nil {
		return nil, ErrNoOutboxConfigured
	}

	tx, err := db.conn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}

	// commit or rollback
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
		} else {
			err = tx.Commit()
		}
	}()

	event, txErr := etxFn(ctx, tx)
	if txErr != nil {
		return nil, txErr
	}

	return db.outbox.EmitEvent(ctx, tx, event)
}

// EventsTx is a convenience method for emitting multiple events in a single transaction.
func (db *PostgresSQL) EventsTx(ctx context.Context, etxFn db.EventsTxFn) (eventIds []events.EventID, err error) {
	if db.outbox == nil {
		return nil, ErrNoOutboxConfigured
	}

	tx, err := db.conn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}

	// commit or rollback
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
		} else {
			err = tx.Commit()
		}
	}()

	eventSpecs, txErr := etxFn(ctx, tx)
	if txErr != nil {
		return nil, txErr
	}

	for _, spec := range eventSpecs {
		eventId, err := db.outbox.EmitEvent(ctx, tx, spec)
		if err != nil {
			return nil, err
		}
		eventIds = append(eventIds, *eventId)
	}

	return
}
