package forwarder

import (
	"time"

	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.uber.org/zap"
)

const (
	DefaultPollingInterval        = 100 * time.Millisecond
	DefaultDirectWorkers          = 8
	DefaultDirectQueueSize        = 1024
	DefaultOutboxDepthSampleEvery = 10
)

type Options struct {
	PollingInterval time.Duration
	Serializer      serialization.Serializer
	OutboxTableName string
	Logger          *zap.Logger

	// DirectEmit enables the push-based happy path: after a successful
	// EventTx/EventsTx commit, events are handed to the forwarder's worker
	// pool for immediate publish + row deletion, bypassing the poller.
	DirectEmit bool
	// DirectWorkers bounds the concurrency of direct emits.
	DirectWorkers int
	// DirectQueueSize is the buffer depth of the direct-emit channel. When
	// full, events are dropped and left for the poller.
	DirectQueueSize int
	// OutboxDepthSampleEvery controls how often (in poller cycles) the
	// outbox table depth is sampled into Metrics.OutboxDepth. Zero disables.
	OutboxDepthSampleEvery int

	// Metrics is optional. When nil the forwarder records nothing. Construct
	// with NewMetrics(mp) to attach to an OpenTelemetry MeterProvider.
	Metrics *Metrics
}

var DefaultOptions = &Options{
	PollingInterval:        DefaultPollingInterval,
	Serializer:             serialization.NewProtobufSerializer(),
	OutboxTableName:        "event_outbox",
	DirectEmit:             false,
	DirectWorkers:          DefaultDirectWorkers,
	DirectQueueSize:        DefaultDirectQueueSize,
	OutboxDepthSampleEvery: DefaultOutboxDepthSampleEvery,
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

	if o.DirectWorkers < 0 {
		o.DirectWorkers = 0
	}
	if o.DirectQueueSize < 0 {
		o.DirectQueueSize = 0
	}

	return nil
}
