package bus

import (
	"math"
	"math/rand/v2"
	"time"
)

// DefaultRetryPolicy retries a failing message 8 times in total with delays of
// 1s, 2s, 4s, … capped at 1m (≈2 min end to end) before it is dead-lettered.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:  8,
	InitialDelay: time.Second,
	MaxDelay:     time.Minute,
	Multiplier:   2,
	Jitter:       0.2,
}

// RetryPolicy controls how often and how fast a message whose handler failed is
// redelivered. A message that fails its MaxAttempts-th delivery is exhausted:
// it is dead-lettered, or dropped when the subscription uses
// WithDropOnExhaustion.
type RetryPolicy struct {
	// MaxAttempts is the total number of deliveries, including the first one.
	MaxAttempts int
	// InitialDelay is the redelivery delay after the first failed delivery.
	InitialDelay time.Duration
	// MaxDelay caps the exponential delay before jitter is applied.
	MaxDelay time.Duration
	// Multiplier grows the delay after every failed delivery.
	Multiplier float64
	// Jitter randomizes each delay by ±Jitter (0.2 = ±20%). Zero disables it.
	Jitter float64
}

// normalize fills unset fields from DefaultRetryPolicy.
func (p RetryPolicy) normalize() RetryPolicy {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = DefaultRetryPolicy.MaxAttempts
	}
	if p.InitialDelay <= 0 {
		p.InitialDelay = DefaultRetryPolicy.InitialDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = DefaultRetryPolicy.MaxDelay
	}
	if p.MaxDelay < p.InitialDelay {
		p.MaxDelay = p.InitialDelay
	}
	if p.Multiplier < 1 {
		p.Multiplier = DefaultRetryPolicy.Multiplier
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter > 1 {
		p.Jitter = 1
	}
	return p
}

// IsExhausted reports whether a message that failed on its numDelivered-th
// delivery has no attempts left.
func (p RetryPolicy) IsExhausted(numDelivered uint64) bool {
	return numDelivered >= uint64(p.MaxAttempts)
}

// Delay returns the redelivery delay after the numDelivered-th delivery failed.
func (p RetryPolicy) Delay(numDelivered uint64) time.Duration {
	if numDelivered < 1 {
		numDelivered = 1
	}

	delay := float64(p.InitialDelay) * math.Pow(p.Multiplier, float64(numDelivered-1))
	if delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}

	if p.Jitter > 0 {
		delay *= 1 + p.Jitter*(2*rand.Float64()-1)
	}

	return time.Duration(delay)
}
