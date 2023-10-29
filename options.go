package strongforce

import (
	"errors"
	"fmt"
	"github.com/vectrum-io/strongforce/pkg/bus/nats"
	"github.com/vectrum-io/strongforce/pkg/db/mysql"
	"github.com/vectrum-io/strongforce/pkg/events"
	"github.com/vectrum-io/strongforce/pkg/forwarder"
	"go.uber.org/zap"
)

var (
	ErrNoDB  = errors.New("no database configured")
	ErrNoBus = errors.New("no bus configured")
)

type clientOptions struct {
	mysqlOptions     *mysql.Options
	forwarderOptions *forwarder.Options
	natsOptions      *nats.Options
	logger           *zap.Logger
}

type Option func(o *clientOptions)

func WithLogger(logger *zap.Logger) Option {
	return func(o *clientOptions) {
		o.logger = logger
	}
}

func WithMySQL(options *mysql.Options) Option {
	return func(o *clientOptions) {
		o.mysqlOptions = options
	}
}

func WithNATS(options *nats.Options) Option {
	return func(o *clientOptions) {
		o.natsOptions = options
	}
}

func WithForwarder(options *forwarder.Options) Option {
	return func(o *clientOptions) {
		o.forwarderOptions = options
	}
}

func (co *clientOptions) CreateClient() (*Client, error) {
	client := &Client{
		eventBuilder: &events.Builder{},
	}

	if co.mysqlOptions != nil {
		db, err := mysql.New(*co.mysqlOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create database: %w", err)
		}
		client.db = db
	}

	if co.natsOptions != nil {
		natsBus, err := nats.New(co.natsOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create database: %w", err)
		}
		client.bus = natsBus
	}

	if co.forwarderOptions != nil {
		if co.mysqlOptions == nil {
			return nil, fmt.Errorf("cannot create forwarder: %w", ErrNoDB)
		}
		if co.natsOptions == nil {
			return nil, fmt.Errorf("cannot create forwarder: %w", ErrNoBus)
		}

		fw, err := forwarder.New(client.db, client.bus, co.forwarderOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create forwarder: %w", err)
		}
		client.forwarder = fw
	}

	return client, nil
}
