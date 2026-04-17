package forwarder

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the Prometheus collectors used by the forwarder. It is safe
// to share a single *Metrics across forwarders — all operations on the
// underlying collectors are concurrency-safe.
//
// Use NewMetrics to construct and register the collectors. Pass nil for the
// forwarder's Metrics option to disable metric recording entirely.
type Metrics struct {
	DirectEnqueued     prometheus.Counter
	DirectPublished    prometheus.Counter
	DirectFailed       prometheus.Counter
	DirectDropped      prometheus.Counter
	DirectDeleteFailed prometheus.Counter

	PollerPublished prometheus.Counter
	PollerFailed    prometheus.Counter

	OutboxDepth        prometheus.Gauge
	EmitLatencySeconds prometheus.Histogram
}

// NewMetrics constructs the forwarder's Prometheus collectors and registers
// them with the provided Registerer. Pass prometheus.DefaultRegisterer to
// attach to the process-wide default registry, or a custom one for test/
// multi-tenant isolation.
//
// Returns the first registration error (e.g. duplicate collector) so the
// caller can surface it during client setup.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	const subsystem = "strongforce_forwarder"

	m := &Metrics{
		DirectEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: subsystem,
			Name:      "direct_enqueued_total",
			Help:      "Events handed to the direct-emit worker pool after a successful commit.",
		}),
		DirectPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: subsystem,
			Name:      "direct_published_total",
			Help:      "Events published to the bus via the direct-emit happy path.",
		}),
		DirectFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: subsystem,
			Name:      "direct_failed_total",
			Help:      "Direct-emit publish attempts that returned an error; events remain in the outbox for the poller to retry.",
		}),
		DirectDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: subsystem,
			Name:      "direct_dropped_total",
			Help:      "Events dropped because the direct-emit queue was full; the poller will pick them up.",
		}),
		DirectDeleteFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: subsystem,
			Name:      "direct_delete_failed_total",
			Help:      "Outbox row deletions that failed after a successful direct publish (may cause a duplicate publish on the next poll).",
		}),
		PollerPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: subsystem,
			Name:      "poller_published_total",
			Help:      "Events published to the bus via the polling path.",
		}),
		PollerFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: subsystem,
			Name:      "poller_failed_total",
			Help:      "Polling publish attempts that returned an error.",
		}),
		OutboxDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: subsystem,
			Name:      "outbox_depth",
			Help:      "Number of rows currently in the outbox table, sampled by the poller.",
		}),
		EmitLatencySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: subsystem,
			Name:      "emit_latency_seconds",
			Help:      "Wall time from direct-emit enqueue to successful bus publish.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 12),
		}),
	}

	if reg != nil {
		collectors := []prometheus.Collector{
			m.DirectEnqueued, m.DirectPublished, m.DirectFailed,
			m.DirectDropped, m.DirectDeleteFailed,
			m.PollerPublished, m.PollerFailed,
			m.OutboxDepth, m.EmitLatencySeconds,
		}
		for _, c := range collectors {
			if err := reg.Register(c); err != nil {
				return nil, err
			}
		}
	}

	return m, nil
}

// The methods below are nil-safe: if the caller passed Metrics: nil in Options
// the forwarder receives a nil *Metrics and these become no-ops. Keeps call
// sites free of `if fw.metrics != nil` guards.

func (m *Metrics) incDirectEnqueued() {
	if m != nil {
		m.DirectEnqueued.Inc()
	}
}

func (m *Metrics) incDirectPublished() {
	if m != nil {
		m.DirectPublished.Inc()
	}
}

func (m *Metrics) incDirectFailed() {
	if m != nil {
		m.DirectFailed.Inc()
	}
}

func (m *Metrics) incDirectDropped() {
	if m != nil {
		m.DirectDropped.Inc()
	}
}

func (m *Metrics) incDirectDeleteFailed() {
	if m != nil {
		m.DirectDeleteFailed.Inc()
	}
}

func (m *Metrics) incPollerPublished() {
	if m != nil {
		m.PollerPublished.Inc()
	}
}

func (m *Metrics) incPollerFailed() {
	if m != nil {
		m.PollerFailed.Inc()
	}
}

func (m *Metrics) setOutboxDepth(v float64) {
	if m != nil {
		m.OutboxDepth.Set(v)
	}
}

func (m *Metrics) observeEmitLatency(seconds float64) {
	if m != nil {
		m.EmitLatencySeconds.Observe(seconds)
	}
}
