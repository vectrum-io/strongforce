package mysql

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/events"
)

var (
	ErrNoOutboxConfigured = errors.New("no event outbox configured")
)

func (db *MySQL) EventTx(ctx context.Context, etxFn db.EventTxFn) (eventId *events.EventID, err error) {
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
				err = errors.Wrap(err, rbError.Error())
			}
		} else if err != nil {
			if rbError := tx.Rollback(); rbError != nil {
				err = errors.Wrap(err, rbError.Error())
			}
		} else {
			err = tx.Commit()
		}
	}()

	event, txErr := etxFn(ctx, tx)
	if txErr != nil {
		return nil, txErr
	}

	return db.outbox.EmitEvent(ctx, tx.Tx, event)
}
