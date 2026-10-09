package bus

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/vectrum-io/strongforce/pkg/bus"

// Outcome is how a subscription settled an inbound message.
type Outcome string

const (
	OutcomeAcked            Outcome = "acked"
	OutcomeSkipped          Outcome = "skipped"
	OutcomeRetried          Outcome = "retried"
	OutcomeDeadLettered     Outcome = "dead_lettered"
	OutcomeDeadLetterFailed Outcome = "dead_letter_failed"
	OutcomeDropped          Outcome = "dropped"
)

// Metrics holds the OpenTelemetry instruments recorded by subscriptions. A nil
// *Metrics records nothing.
type Metrics struct {
	Messages        metric.Int64Counter
	HandlerDuration metric.Float64Histogram
	Pinned          metric.Int64UpDownCounter
}

// NewMetrics constructs the subscription instruments against mp.
func NewMetrics(mp metric.MeterProvider) (*Metrics, error) {
	if mp == nil {
		return nil, fmt.Errorf("nil MeterProvider")
	}
	meter := mp.Meter(meterName)

	messages, err := meter.Int64Counter(
		"strongforce.bus.messages",
		metric.WithDescription("Inbound messages by how the subscription settled them (acked, skipped, retried, dead_lettered, dead_letter_failed, dropped)."),
	)
	if err != nil {
		return nil, err
	}

	handlerDuration, err := meter.Float64Histogram(
		"strongforce.bus.handler.duration",
		metric.WithDescription("Wall time spent in the handlers of one inbound message."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
		),
	)
	if err != nil {
		return nil, err
	}

	pinned, err := meter.Int64UpDownCounter(
		"strongforce.bus.pinned",
		metric.WithDescription("Subscriptions that hold the pin of their priority group."),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		Messages:        messages,
		HandlerDuration: handlerDuration,
		Pinned:          pinned,
	}, nil
}

func (m *Metrics) recordOutcome(ctx context.Context, stream, consumer string, outcome Outcome) {
	if m == nil {
		return
	}
	m.Messages.Add(ctx, 1, metric.WithAttributes(
		attribute.String("stream", stream),
		attribute.String("consumer", consumer),
		attribute.String("outcome", string(outcome)),
	))
}

func (m *Metrics) recordHandlerDuration(ctx context.Context, stream, consumer string, d time.Duration, failed bool) {
	if m == nil {
		return
	}
	result := "success"
	if failed {
		result = "failure"
	}
	m.HandlerDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("stream", stream),
		attribute.String("consumer", consumer),
		attribute.String("result", result),
	))
}

// RecordPinned counts a subscription that gained (pinned) or lost the pin of
// its priority group.
func (m *Metrics) RecordPinned(ctx context.Context, stream, consumer, group string, pinned bool) {
	if m == nil || m.Pinned == nil {
		return
	}
	delta := int64(-1)
	if pinned {
		delta = 1
	}
	m.Pinned.Add(ctx, delta, metric.WithAttributes(
		attribute.String("stream", stream),
		attribute.String("consumer", consumer),
		attribute.String("group", group),
	))
}
