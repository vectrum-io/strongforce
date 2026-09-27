package bus

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryPolicyDelayDoublesUntilCap(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  8,
		InitialDelay: time.Second,
		MaxDelay:     time.Minute,
		Multiplier:   2,
	}.normalize()

	expected := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		time.Minute,
		time.Minute,
	}
	for i, want := range expected {
		assert.Equalf(t, want, policy.Delay(uint64(i+1)), "delay after delivery %d", i+1)
	}
}

func TestRetryPolicyDelayTreatsUntrackedDeliveryAsFirst(t *testing.T) {
	policy := RetryPolicy{InitialDelay: 3 * time.Second, Multiplier: 2}.normalize()

	assert.Equal(t, 3*time.Second, policy.Delay(0))
}

func TestRetryPolicyJitterStaysWithinBounds(t *testing.T) {
	policy := RetryPolicy{
		InitialDelay: 10 * time.Second,
		MaxDelay:     10 * time.Second,
		Multiplier:   2,
		Jitter:       0.2,
	}.normalize()

	for i := 0; i < 1000; i++ {
		delay := policy.Delay(3)
		assert.GreaterOrEqual(t, delay, 8*time.Second)
		assert.LessOrEqual(t, delay, 12*time.Second)
	}
}

func TestRetryPolicyNormalizeFillsDefaults(t *testing.T) {
	policy := RetryPolicy{}.normalize()

	assert.Equal(t, DefaultRetryPolicy.MaxAttempts, policy.MaxAttempts)
	assert.Equal(t, DefaultRetryPolicy.InitialDelay, policy.InitialDelay)
	assert.Equal(t, DefaultRetryPolicy.MaxDelay, policy.MaxDelay)
	assert.Equal(t, DefaultRetryPolicy.Multiplier, policy.Multiplier)
	assert.Zero(t, policy.Jitter)
}

func TestRetryPolicyNormalizeClampsInvalidValues(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  -3,
		InitialDelay: time.Minute,
		MaxDelay:     time.Second,
		Multiplier:   0.5,
		Jitter:       4,
	}.normalize()

	assert.Equal(t, DefaultRetryPolicy.MaxAttempts, policy.MaxAttempts)
	assert.Equal(t, time.Minute, policy.MaxDelay)
	assert.Equal(t, DefaultRetryPolicy.Multiplier, policy.Multiplier)
	assert.Equal(t, 1.0, policy.Jitter)
}

func TestRetryPolicyIsExhaustedOnLastAttempt(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3}.normalize()

	assert.False(t, policy.IsExhausted(0))
	assert.False(t, policy.IsExhausted(2))
	assert.True(t, policy.IsExhausted(3))
	assert.True(t, policy.IsExhausted(4))
}

func TestPermanentSurvivesWrapping(t *testing.T) {
	base := errors.New("incident not found")
	wrapped := fmt.Errorf("handle created: %w", Permanent(base))

	assert.True(t, IsPermanent(wrapped))
	assert.ErrorIs(t, wrapped, base)
	assert.Equal(t, "handle created: incident not found", wrapped.Error())
}

func TestPermanentIgnoresNil(t *testing.T) {
	assert.NoError(t, Permanent(nil))
}

func TestIsPermanentRejectsPlainErrors(t *testing.T) {
	assert.False(t, IsPermanent(errors.New("timeout")))
	assert.False(t, IsPermanent(nil))
}

func TestIsPermanentFindsJoinedPermanentError(t *testing.T) {
	joined := errors.Join(errors.New("timeout"), Permanent(errors.New("bad payload")))

	assert.True(t, IsPermanent(joined))
}
