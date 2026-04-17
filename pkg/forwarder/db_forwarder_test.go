package forwarder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDirectEmitRequiresDB(t *testing.T) {
	_, err := New(nil, nil, &Options{DirectEmit: true})
	assert.ErrorIs(t, err, ErrDirectEmitRequiresDB)
}

func TestNewWithoutDirectEmitAllowsNilDB(t *testing.T) {
	// The polling path also needs a db at runtime, but New itself should not
	// reject nil inputs unless DirectEmit is on — preserves the existing
	// behavior for callers that build forwarders in tests or custom setups.
	_, err := New(nil, nil, &Options{})
	assert.False(t, errors.Is(err, ErrDirectEmitRequiresDB))
	assert.False(t, errors.Is(err, ErrDirectEmitRequiresBus))
}
