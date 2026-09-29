package bus

import (
	"time"

	"github.com/vectrum-io/strongforce/pkg/serialization"
)

// DefaultConcurrency is the per-subscription worker count used when the caller
// neither opts into WithGuaranteeOrder nor specifies WithConcurrency. Eight is
// a conservative default for I/O-bound handlers (DB writes, downstream RPCs):
// enough parallelism that one slow message doesn't stall the queue, low enough
// that surprise concurrent handler invocations are unlikely to overwhelm a
// downstream resource. Callers with high-throughput needs (e.g. the
// monitor-executor running long HTTP probes) should set WithConcurrency
// explicitly.
const DefaultConcurrency = 8

var DefaultSubscriptionOptions = SubscriptionOptions{
	FilterSubjects: []string{},
	GuaranteeOrder: false,
	RetryPolicy:    DefaultRetryPolicy,
}

type SubscriptionOptions struct {
	FilterSubjects []string
	GuaranteeOrder bool
	// RetryPolicy controls redelivery of messages whose handler failed.
	RetryPolicy RetryPolicy
	// MaxDeliveryTries overrides RetryPolicy.MaxAttempts when non-zero.
	//
	// Deprecated: use RetryPolicy.MaxAttempts or WithMaxDeliveryTries.
	MaxDeliveryTries int
	// DropOnExhaustion drops messages that used up all attempts instead of
	// dead-lettering them.
	DropOnExhaustion bool
	DeliveryPolicy   DeliveryPolicy
	Durable          bool
	Deserializer     serialization.Serializer
	// Concurrency is the number of goroutines that race to handle inbound
	// messages. Zero means "use the default": 1 when GuaranteeOrder is set,
	// DefaultConcurrency otherwise. GuaranteeOrder always forces 1 — concurrent
	// handlers cannot preserve stream order.
	Concurrency int
	// AckWait overrides the JetStream consumer's AckWait. Zero leaves it at the
	// server default (30 s). Raise this when a handler can legitimately exceed
	// 30 s — e.g. a probe with a long timeout — to prevent JetStream from
	// redelivering a message that's still being processed.
	AckWait time.Duration
}

type DeliveryPolicy int

const (
	DeliverAll DeliveryPolicy = iota
	DeliverNew
)

type SubscribeOption func(*SubscriptionOptions)

func WithFilterSubject(subject string) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.FilterSubjects = append(options.FilterSubjects, subject)
	}
}

// WithFilterSubjects appends each subject to the consumer's FilterSubjects list.
// Use when a single consumer needs to filter on multiple subjects (NATS >= 2.10).
// Equivalent to chaining WithFilterSubject calls but clearer at the call site.
func WithFilterSubjects(subjects ...string) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.FilterSubjects = append(options.FilterSubjects, subjects...)
	}
}

func WithGuaranteeOrder() SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.GuaranteeOrder = true
	}
}

// EffectiveRetryPolicy is RetryPolicy with the deprecated MaxDeliveryTries
// applied.
func (o SubscriptionOptions) EffectiveRetryPolicy() RetryPolicy {
	policy := o.RetryPolicy
	if o.MaxDeliveryTries != 0 {
		policy.MaxAttempts = o.MaxDeliveryTries
	}
	return policy
}

// WithMaxDeliveryTries sets the total number of deliveries (RetryPolicy.MaxAttempts)
// before a message is dead-lettered or dropped. A negative value retries
// without limit.
func WithMaxDeliveryTries(maxTries int) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.RetryPolicy.MaxAttempts = maxTries
	}
}

// WithRetryPolicy replaces the subscription's retry policy. Unset fields fall
// back to DefaultRetryPolicy.
func WithRetryPolicy(policy RetryPolicy) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.RetryPolicy = policy
	}
}

// WithDropOnExhaustion drops messages that used up all attempts instead of
// dead-lettering them. Use it for messages that are worthless once stale, such
// as heartbeats.
func WithDropOnExhaustion() SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.DropOnExhaustion = true
	}
}

func WithDeliveryPolicy(policy DeliveryPolicy) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.DeliveryPolicy = policy
	}
}

func WithDurable() SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.Durable = true
	}
}

// WithConcurrency sets the per-subscription handler-goroutine count. It also
// determines the JetStream consumer's MaxAckPending so the in-flight set
// matches what the workers can actually process (preventing AckWait expiry on
// queued messages). Ignored when WithGuaranteeOrder is set — ordered delivery
// must remain single-goroutine.
func WithConcurrency(n int) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.Concurrency = n
	}
}

// WithAckWait sets the JetStream consumer's AckWait. JetStream redelivers a
// message if it isn't acked within this window, so it must be larger than the
// p99 handler duration. Default (zero) leaves the server's 30 s in place.
func WithAckWait(d time.Duration) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.AckWait = d
	}
}

func WithDeserializer(deserializer serialization.Serializer) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.Deserializer = deserializer
	}
}
