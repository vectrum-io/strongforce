package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
)

type PostgresSQL struct {
	conn              *sqlx.DB
	migrator          db.Migrator
	dsn               string
	outbox            *outbox.Outbox
	logger            *zap.Logger
	connectionOptions *ConnectionOptions
}

func New(options Options) (*PostgresSQL, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate db options: %w", err)
	}

	ob, err := outbox.New(options.OutboxOptions)
	if err != nil {
		return nil, err
	}

	return &PostgresSQL{
		dsn:               options.DSN,
		migrator:          options.Migrator,
		outbox:            ob,
		logger:            options.Logger,
		connectionOptions: options.ConnectionOptions,
	}, nil
}

func (db *PostgresSQL) Connect() error {
	connection, err := otelsql.Open("postgres", db.dsn, otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	if err != nil {
		return err
	}

	connection.SetConnMaxLifetime(db.connectionOptions.ConnMaxLifetime)
	connection.SetMaxIdleConns(db.connectionOptions.MaxIdleCons)
	connection.SetMaxOpenConns(db.connectionOptions.MaxOpenCons)
	connection.SetConnMaxIdleTime(db.connectionOptions.ConnMaxIdleTime)

	db.conn = sqlx.NewDb(connection, "postgres")

	return nil
}

func (db *PostgresSQL) Close() error {
	return db.conn.Close()
}

func (db *PostgresSQL) Connection() *sqlx.DB {
	return db.conn
}

func (db *PostgresSQL) Migrate(ctx context.Context, options *db.MigrationOptions) (*db.MigrationResult, error) {
	if db.migrator == nil {
		return nil, ErrNoMigrator
	}

	if options.DropTablesBeforeMigrating {
		if db.Connection() == nil {
			return nil, fmt.Errorf("cannot drop tables as no connection exists")
		}

		var tables []string
		if err := db.Connection().Select(&tables, "SHOW TABLES"); err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}

		if _, err := db.Connection().Exec("SET session_replication_role = 'replica'"); err != nil {
			return nil, fmt.Errorf("failed to set session_replicaiton_role to replica: %w", err)
		}

		for _, table := range tables {
			if _, err := db.Connection().Exec("DROP TABLE " + table); err != nil {
				return nil, fmt.Errorf("failed to drop table %s: %w", table, err)
			}
		}

		if _, err := db.Connection().Exec("SET session_replication_role = 'origin'"); err != nil {
			return nil, fmt.Errorf("failed to set session_replicaiton_role to origin: %w", err)
		}
	}

	return db.migrator.Migrate(ctx, db.dsn)
}
