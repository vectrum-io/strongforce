package forwarder

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

// meterName is the instrumentation scope attached to every metric created by
// this package. Consumers can filter on it in their OTel pipelines.
const meterName = "github.com/vectrum-io/strongforce/pkg/forwarder"

// Metrics holds the OpenTelemetry instruments used by the forwarder. It is
// safe to share a single *Metrics across forwarders — all operations on the
// underlying instruments are concurrency-safe.
//
// Use NewMetrics to construct against a MeterProvider. Pass nil for the
// forwarder's Metrics option to disable metric recording entirely.
type Metrics struct {
	DirectEnqueued     metric.Int64Counter
	DirectPublished    metric.Int64Counter
	DirectFailed       metric.Int64Counter
	DirectDropped      metric.Int64Counter
	DirectDeleteFailed metric.Int64Counter

	PollerPublished metric.Int64Counter
	PollerFailed    metric.Int64Counter

	OutboxDepth        metric.Int64Gauge
	EmitLatencySeconds metric.Float64Histogram
}

// NewMetrics constructs the forwarder's OTel instruments against the provided
// MeterProvider. Pass otel.GetMeterProvider() to attach to the process-wide
// global provider, or a custom one for test/multi-tenant isolation.
//
// Returns the first instrument-creation error so the caller can surface it
// during client setup.
func NewMetrics(mp metric.MeterProvider) (*Metrics, error) {
	if mp == nil {
		return nil, fmt.Errorf("nil MeterProvider")
	}
	meter := mp.Meter(meterName)

	directEnqueued, err := meter.Int64Counter(
		"strongforce.forwarder.direct.enqueued",
		metric.WithDescription("Events handed to the direct-emit worker pool after a successful commit."),
	)
	if err != nil {
		return nil, err
	}
	directPublished, err := meter.Int64Counter(
		"strongforce.forwarder.direct.published",
		metric.WithDescription("Events published to the bus via the direct-emit happy path."),
	)
	if err != nil {
		return nil, err
	}
	directFailed, err := meter.Int64Counter(
		"strongforce.forwarder.direct.failed",
		metric.WithDescription("Direct-emit publish attempts that returned an error; events remain in the outbox for the poller to retry."),
	)
	if err != nil {
		return nil, err
	}
	directDropped, err := meter.Int64Counter(
		"strongforce.forwarder.direct.dropped",
		metric.WithDescription("Events dropped because the direct-emit queue was full; the poller will pick them up."),
	)
	if err != nil {
		return nil, err
	}
	directDeleteFailed, err := meter.Int64Counter(
		"strongforce.forwarder.direct.delete_failed",
		metric.WithDescription("Outbox row deletions that failed after a successful direct publish (may cause a duplicate publish on the next poll)."),
	)
	if err != nil {
		return nil, err
	}
	pollerPublished, err := meter.Int64Counter(
		"strongforce.forwarder.poller.published",
		metric.WithDescription("Events published to the bus via the polling path."),
	)
	if err != nil {
		return nil, err
	}
	pollerFailed, err := meter.Int64Counter(
		"strongforce.forwarder.poller.failed",
		metric.WithDescription("Polling publish attempts that returned an error."),
	)
	if err != nil {
		return nil, err
	}
	outboxDepth, err := meter.Int64Gauge(
		"strongforce.forwarder.outbox.depth",
		metric.WithDescription("Number of rows currently in the outbox table, sampled by the poller."),
	)
	if err != nil {
		return nil, err
	}
	emitLatency, err := meter.Float64Histogram(
		"strongforce.forwarder.emit.latency",
		metric.WithDescription("Wall time from direct-emit enqueue to successful bus publish."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.0005, 0.001, 0.002, 0.004, 0.008, 0.016,
			0.032, 0.064, 0.128, 0.256, 0.512, 1.024,
		),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		DirectEnqueued:     directEnqueued,
		DirectPublished:    directPublished,
		DirectFailed:       directFailed,
		DirectDropped:      directDropped,
		DirectDeleteFailed: directDeleteFailed,
		PollerPublished:    pollerPublished,
		PollerFailed:       pollerFailed,
		OutboxDepth:        outboxDepth,
		EmitLatencySeconds: emitLatency,
	}, nil
}

// The methods below are nil-safe: if the caller passed Metrics: nil in Options
// the forwarder receives a nil *Metrics and these become no-ops. Keeps call
// sites free of `if fw.metrics != nil` guards.

func (m *Metrics) incDirectEnqueued(ctx context.Context) {
	if m != nil {
		m.DirectEnqueued.Add(ctx, 1)
	}
}

func (m *Metrics) incDirectPublished(ctx context.Context) {
	if m != nil {
		m.DirectPublished.Add(ctx, 1)
	}
}

func (m *Metrics) incDirectFailed(ctx context.Context) {
	if m != nil {
		m.DirectFailed.Add(ctx, 1)
	}
}

func (m *Metrics) incDirectDropped(ctx context.Context) {
	if m != nil {
		m.DirectDropped.Add(ctx, 1)
	}
}

func (m *Metrics) incDirectDeleteFailed(ctx context.Context) {
	if m != nil {
		m.DirectDeleteFailed.Add(ctx, 1)
	}
}

func (m *Metrics) incPollerPublished(ctx context.Context) {
	if m != nil {
		m.PollerPublished.Add(ctx, 1)
	}
}

func (m *Metrics) incPollerFailed(ctx context.Context) {
	if m != nil {
		m.PollerFailed.Add(ctx, 1)
	}
}

func (m *Metrics) setOutboxDepth(ctx context.Context, v int64) {
	if m != nil {
		m.OutboxDepth.Record(ctx, v)
	}
}

func (m *Metrics) observeEmitLatency(ctx context.Context, seconds float64) {
	if m != nil {
		m.EmitLatencySeconds.Record(ctx, seconds)
	}
}
