package mysql

import (
	"context"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
)

type MySQL struct {
	conn     *sqlx.DB
	migrator db.Migrator
	config   *mysql.Config
	outbox   *outbox.Outbox
	logger   *zap.Logger
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
		config:   cfg,
		migrator: options.Migrator,
		outbox:   ob,
		logger:   options.Logger,
	}, nil
}

func (db *MySQL) Connect() error {
	connection, err := otelsql.Open("mysql", db.config.FormatDSN(), otelsql.WithAttributes(semconv.DBSystemMySQL), otelsql.WithDBName(db.config.DBName))
	if err != nil {
		return err
	}

	db.conn = sqlx.NewDb(connection, "mysql")
	return nil
}

func (db *MySQL) Close() error {
	return db.conn.Close()
}

func (db *MySQL) Connection() *sqlx.DB {
	return db.conn
}

func (db *MySQL) Migrate(ctx context.Context) error {
	if db.migrator == nil {
		return ErrNoMigrator
	}

	dsn := fmt.Sprintf("mysql://%s:%s@%s/%s", db.config.User, db.config.Passwd, db.config.Addr, db.config.DBName)

	return db.migrator.Migrate(ctx, dsn)
}
