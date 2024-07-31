package mysql

import (
	"errors"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	"go.uber.org/zap"
	"time"
)

var (
	ErrNoDSN      = errors.New("no dsn provided")
	ErrNoMigrator = errors.New("no migrator provided")
)

var DefaultConnectionOptions = ConnectionOptions{
	MaxOpenCons:     100,
	MaxIdleCons:     10,
	ConnMaxLifetime: 1 * time.Minute,
	ConnMaxIdleTime: 30 * time.Second,
}

type Options struct {
	DSN               string
	OutboxOptions     *outbox.Options
	Logger            *zap.Logger
	Migrator          db.Migrator
	ConnectionOptions *ConnectionOptions
}

type ConnectionOptions struct {
	MaxOpenCons     int
	MaxIdleCons     int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func (o *Options) validate() error {
	if o.DSN == "" {
		return ErrNoDSN
	}

	if o.Logger == nil {
		o.Logger = zap.L()
	}

	if o.OutboxOptions == nil {
		o.OutboxOptions = &outbox.Options{}
	}

	if o.ConnectionOptions == nil {
		o.ConnectionOptions = &DefaultConnectionOptions
	}

	return nil
}
