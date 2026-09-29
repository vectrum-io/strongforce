package bus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEffectiveRetryPolicyAppliesDeprecatedMaxDeliveryTries(t *testing.T) {
	options := DefaultSubscriptionOptions
	options.MaxDeliveryTries = -1

	policy := options.EffectiveRetryPolicy()

	assert.Equal(t, -1, policy.MaxAttempts)
	assert.Equal(t, DefaultRetryPolicy.InitialDelay, policy.InitialDelay)
}

func TestEffectiveRetryPolicyKeepsRetryPolicyWithoutMaxDeliveryTries(t *testing.T) {
	options := DefaultSubscriptionOptions
	WithMaxDeliveryTries(3)(&options)

	assert.Equal(t, 3, options.EffectiveRetryPolicy().MaxAttempts)
}
