package nats

import (
	"testing"

	"github.com/hashicorp/go-version"
	"github.com/stretchr/testify/assert"
)

func TestSubscribeOptsLeaveDeliveryLimitToRetryPolicy(t *testing.T) {
	opts := SubscribeOpts{}

	assert.NoError(t, opts.validate(version.Must(version.NewVersion("2.10.0"))))

	assert.Equal(t, -1, opts.MaxDeliverTries)
}

func TestSubscribeOptsKeepExplicitDeliveryLimit(t *testing.T) {
	opts := SubscribeOpts{MaxDeliverTries: 5}

	assert.NoError(t, opts.validate(version.Must(version.NewVersion("2.10.0"))))

	assert.Equal(t, 5, opts.MaxDeliverTries)
}
