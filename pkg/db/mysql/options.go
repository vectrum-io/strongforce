package mysql

import (
	"errors"
	"github.com/vectrum-io/strongforce/pkg/db"
	"github.com/vectrum-io/strongforce/pkg/outbox"
	"go.uber.org/zap"
)

var (
	ErrNoDSN      = errors.New("no dsn provided")
	ErrNoMigrator = errors.New("no migrator provided")
)

type Options struct {
	DSN           string
	OutboxOptions *outbox.Options
	Logger        *zap.Logger
	Migrator      db.Migrator
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

	return nil
}
