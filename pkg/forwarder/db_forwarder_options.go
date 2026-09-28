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
	DefaultPollerBatchSize        = 100
	DefaultPollerBatchBudget      = 5 * time.Second
	DefaultPollerGracePeriod      = 10 * time.Second
	DefaultPollerMaxBackoff       = time.Minute
	DefaultPublishTimeout         = 2 * time.Second
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
	// outbox table depth and oldest row age are sampled into Metrics. Zero
	// disables.
	OutboxDepthSampleEvery int

	// PollerBatchSize bounds how many rows one poll locks and publishes. A
	// full batch triggers the next poll right away.
	PollerBatchSize int
	// PollerBatchBudget bounds how long one poll keeps publishing, and so how
	// long it holds its row locks. Rows left over are picked up by the next
	// poll right away. Rows are published one at a time to keep their order.
	PollerBatchBudget time.Duration
	// PollerGracePeriod makes the poller skip rows younger than this, so it
	// does not race the direct-emit workers on fresh events. It only applies
	// with DirectEmit and relies on ULID event ids. Zero uses the default,
	// negative disables it.
	PollerGracePeriod time.Duration
	// PollerMaxBackoff caps how far the polling interval grows while
	// publishing keeps failing.
	PollerMaxBackoff time.Duration
	// PublishTimeout bounds every single publish, so an unreachable bus
	// fails fast instead of holding the poller's row locks.
	PublishTimeout time.Duration

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
	PollerBatchSize:        DefaultPollerBatchSize,
	PollerBatchBudget:      DefaultPollerBatchBudget,
	PollerGracePeriod:      DefaultPollerGracePeriod,
	PollerMaxBackoff:       DefaultPollerMaxBackoff,
	PublishTimeout:         DefaultPublishTimeout,
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

	if o.PollerBatchSize <= 0 {
		o.PollerBatchSize = DefaultPollerBatchSize
	}

	if o.PollerBatchBudget <= 0 {
		o.PollerBatchBudget = DefaultPollerBatchBudget
	}

	if o.PollerGracePeriod == 0 {
		o.PollerGracePeriod = DefaultPollerGracePeriod
	}

	if o.PollerMaxBackoff <= 0 {
		o.PollerMaxBackoff = DefaultPollerMaxBackoff
	}
	if o.PollerMaxBackoff < o.PollingInterval {
		o.PollerMaxBackoff = o.PollingInterval
	}

	if o.PublishTimeout <= 0 {
		o.PublishTimeout = DefaultPublishTimeout
	}

	return nil
}
