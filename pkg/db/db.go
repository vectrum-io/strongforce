package db

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/vectrum-io/strongforce/pkg/events"
)

type DB interface {
	Connect() error
	Close() error
	Connection() *sqlx.DB
	Migrate(ctx context.Context, options *MigrationOptions) (*MigrationResult, error)
	Tx(ctx context.Context, tx TxFn) error
	EventTx(ctx context.Context, etx EventTxFn) (*events.EventID, error)
	EventsTx(ctx context.Context, etx EventsTxFn) ([]events.EventID, error)
}

type MigrationOptions struct {
	DropTablesBeforeMigrating bool
}

type Migrator interface {
	Migrate(ctx context.Context, dsn string) (*MigrationResult, error)
}

type MigrationResult struct {
	AppliedMigrations []string
}

type TxFn func(ctx context.Context, tx *sqlx.Tx) error
type EventTxFn func(ctx context.Context, tx *sqlx.Tx) (*events.EventSpec, error)
type EventsTxFn func(ctx context.Context, tx *sqlx.Tx) ([]*events.EventSpec, error)
