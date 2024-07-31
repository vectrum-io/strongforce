package mysql

import (
	"context"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
)

type MySQL struct {
	conn              *sqlx.DB
	migrator          db.Migrator
	config            *mysql.Config
	connectionOptions *ConnectionOptions
	outbox            *outbox.Outbox
	logger            *zap.Logger
}

func New(options Options) (*MySQL, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("failed to validate db options: %w", err)
	}

	cfg, err := mysql.ParseDSN(options.DSN)
	if err != nil {
		return nil, err
	}

	ob, err := outbox.New(options.OutboxOptions)
	if err != nil {
		return nil, err
	}

	return &MySQL{
		config:            cfg,
		migrator:          options.Migrator,
		outbox:            ob,
		logger:            options.Logger,
		connectionOptions: options.ConnectionOptions,
	}, nil
}

func (db *MySQL) Connect() error {
	connection, err := otelsqlx.Open("mysql", db.config.FormatDSN(), otelsql.WithAttributes(semconv.DBSystemMySQL), otelsql.WithDBName(db.config.DBName))
	if err != nil {
		return err
	}

	connection.SetConnMaxLifetime(db.connectionOptions.ConnMaxLifetime)
	connection.SetMaxIdleConns(db.connectionOptions.MaxIdleCons)
	connection.SetMaxOpenConns(db.connectionOptions.MaxOpenCons)
	connection.SetConnMaxIdleTime(db.connectionOptions.ConnMaxIdleTime)

	db.conn = connection
	return nil
}

func (db *MySQL) Close() error {
	return db.conn.Close()
}

func (db *MySQL) Connection() *sqlx.DB {
	return db.conn
}

func (db *MySQL) Migrate(ctx context.Context, options *db.MigrationOptions) (*db.MigrationResult, error) {
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

		if _, err := db.Connection().Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			return nil, fmt.Errorf("failed to disable foreign key checks: %w", err)
		}

		for _, table := range tables {
			if _, err := db.Connection().Exec("DROP TABLE " + table); err != nil {
				return nil, fmt.Errorf("failed to drop table %s: %w", table, err)
			}
		}

		if _, err := db.Connection().Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			return nil, fmt.Errorf("failed to enable foreign key checks: %w", err)
		}
	}

	dsn := fmt.Sprintf("mysql://%s:%s@%s/%s", db.config.User, db.config.Passwd, db.config.Addr, db.config.DBName)

	return db.migrator.Migrate(ctx, dsn)
}
