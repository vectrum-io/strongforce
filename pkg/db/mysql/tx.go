package mysql

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vectrum-io/strongforce/pkg/db"
)

func (db *MySQL) Tx(ctx context.Context, txFn db.TxFn) (err error) {
	tx, err := db.conn.BeginTxx(ctx, nil)
	if err != nil {
		return err
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

	return txFn(ctx, tx)
}
