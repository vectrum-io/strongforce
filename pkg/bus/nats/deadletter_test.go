package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestDeadLetterStreamConfigUsesDefaultsOnCreate(t *testing.T) {
	config := DeadLetterOptions{}.streamConfig(nil)

	assert.Equal(t, 30*24*time.Hour, config.MaxAge)
	assert.Equal(t, int64(1024*1024*1024), config.MaxBytes)
	assert.Zero(t, config.Replicas)
}

func TestDeadLetterStreamConfigKeepsExistingValuesForUnsetFields(t *testing.T) {
	existing := &nats.StreamConfig{MaxAge: 90 * 24 * time.Hour, MaxBytes: 10 << 30, Replicas: 3}

	config := DeadLetterOptions{}.streamConfig(existing)

	assert.Equal(t, 90*24*time.Hour, config.MaxAge)
	assert.Equal(t, int64(10<<30), config.MaxBytes)
	assert.Equal(t, 3, config.Replicas)
}

func TestDeadLetterStreamConfigAppliesConfiguredFieldsOnUpdate(t *testing.T) {
	existing := &nats.StreamConfig{MaxAge: 90 * 24 * time.Hour, MaxBytes: 10 << 30, Replicas: 3}

	config := DeadLetterOptions{MaxAge: 7 * 24 * time.Hour}.streamConfig(existing)

	assert.Equal(t, 7*24*time.Hour, config.MaxAge)
	assert.Equal(t, int64(10<<30), config.MaxBytes)
	assert.Equal(t, 3, config.Replicas)
}
