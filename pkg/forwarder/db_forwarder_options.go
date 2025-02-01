package forwarder

import (
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.uber.org/zap"
	"time"
)

const DefaultPollingInterval = 100 * time.Millisecond

type Options struct {
	PollingInterval time.Duration
	Serializer      serialization.Serializer
	OutboxTableName string
	Logger          *zap.Logger
}

var DefaultOptions = &Options{
	PollingInterval: DefaultPollingInterval,
	Serializer:      serialization.NewProtobufSerializer(),
	OutboxTableName: "event_outbox",
}

func (o *Options) validate() error {
	if o.OutboxTableName == "" {
		o.OutboxTableName = DefaultOptions.OutboxTableName
	}

	if o.PollingInterval == 0 {
		o.PollingInterval = DefaultOptions.PollingInterval
	}

	if o.Serializer == nil {
		o.Serializer = DefaultOptions.Serializer
	}

	if o.Logger == nil {
		o.Logger = zap.L()
	}

	return nil
}
