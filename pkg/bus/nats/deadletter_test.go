package nats

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/vectrum-io/strongforce/pkg/bus"
)

func TestDeadLetterHeaders(t *testing.T) {
	message := bus.InboundMessage{
		Id:      "msg-1",
		Subject: "incidents.v1.created",
		Headers: map[string][]string{
			"Traceparent": {"00-trace"},
			"Nats-Msg-Id": {"msg-1"},
		},
		Delivery: bus.DeliveryInfo{Stream: "incidents", Consumer: "notifications", StreamSequence: 7, NumDelivered: 3},
	}

	t.Run("keeps the message headers except the NATS ones and adds the failure", func(t *testing.T) {
		headers := deadLetterHeaders(message, errors.New("downstream unavailable"))

		assert.Equal(t, "00-trace", headers.Get("Traceparent"))
		assert.Empty(t, headers.Get("Nats-Msg-Id"))
		assert.Equal(t, "downstream unavailable", headers.Get(DeadLetterHeaderError))
		assert.Equal(t, "incidents", headers.Get(DeadLetterHeaderStream))
		assert.Equal(t, "notifications", headers.Get(DeadLetterHeaderConsumer))
		assert.Equal(t, "7", headers.Get(DeadLetterHeaderStreamSequence))
		assert.Equal(t, "3", headers.Get(DeadLetterHeaderNumDelivered))
	})

	t.Run("records an unknown cause when none is given", func(t *testing.T) {
		headers := deadLetterHeaders(message, nil)

		assert.Equal(t, "unknown", headers.Get(DeadLetterHeaderError))
	})
}

func TestSanitizeHeaderValue(t *testing.T) {
	t.Run("replaces line breaks", func(t *testing.T) {
		assert.Equal(t, "panic | goroutine 1 |  frame", sanitizeHeaderValue("panic\ngoroutine 1\r\n frame"))
	})

	t.Run("truncates long values on a rune boundary", func(t *testing.T) {
		value := strings.Repeat("a", maxDeadLetterErrorLength-1) + "ü" + "tail"

		sanitized := sanitizeHeaderValue(value)

		assert.True(t, utf8.ValidString(sanitized))
		assert.LessOrEqual(t, len(sanitized), maxDeadLetterErrorLength)
		assert.Equal(t, strings.Repeat("a", maxDeadLetterErrorLength-1), sanitized)
	})

	t.Run("keeps short values", func(t *testing.T) {
		assert.Equal(t, "boom", sanitizeHeaderValue("boom"))
	})
}
