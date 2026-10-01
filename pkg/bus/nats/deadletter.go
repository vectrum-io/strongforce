package nats

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/vectrum-io/strongforce/pkg/bus"
)

const (
	// DeadLetterStreamName is reserved: Migrate owns the stream and configures
	// it from Options.DeadLetter, so it must not appear in Options.Streams.
	DeadLetterStreamName    = "dead_letters"
	DeadLetterSubjectPrefix = "dlq"

	DeadLetterHeaderError          = "Strongforce-Dlq-Error"
	DeadLetterHeaderStream         = "Strongforce-Dlq-Stream"
	DeadLetterHeaderConsumer       = "Strongforce-Dlq-Consumer"
	DeadLetterHeaderSubject        = "Strongforce-Dlq-Subject"
	DeadLetterHeaderMessageId      = "Strongforce-Dlq-Message-Id"
	DeadLetterHeaderStreamSequence = "Strongforce-Dlq-Stream-Sequence"
	DeadLetterHeaderNumDelivered   = "Strongforce-Dlq-Num-Delivered"
	DeadLetterHeaderFailedAt       = "Strongforce-Dlq-Failed-At"

	maxDeadLetterErrorLength = 2048
	deadLetterPublishTimeout = 10 * time.Second
)

// DeadLetterOptions configures the shared dead-letter stream. Every service
// migrates the same stream, so a field left zero keeps the stream's current
// value and only falls back to its default when the stream is created.
type DeadLetterOptions struct {
	// MaxAge bounds how long dead letters are kept. Defaults to 30 days.
	MaxAge time.Duration
	// MaxBytes caps the stream size; oldest dead letters are discarded first.
	// Defaults to 1 GiB.
	MaxBytes int64
	// Replicas of the stream. Zero leaves the server default.
	Replicas int
}

// streamConfig builds the dead-letter stream config. existing is the config of
// the stream already on the server, or nil when it is about to be created.
func (o DeadLetterOptions) streamConfig(existing *nats.StreamConfig) nats.StreamConfig {
	maxAge := o.MaxAge
	maxBytes := o.MaxBytes
	replicas := o.Replicas

	if existing != nil {
		if maxAge <= 0 {
			maxAge = existing.MaxAge
		}
		if maxBytes <= 0 {
			maxBytes = existing.MaxBytes
		}
		if replicas <= 0 {
			replicas = existing.Replicas
		}
	} else {
		if maxAge <= 0 {
			maxAge = 30 * 24 * time.Hour
		}
		if maxBytes <= 0 {
			maxBytes = 1024 * 1024 * 1024
		}
	}

	return nats.StreamConfig{
		Name:        DeadLetterStreamName,
		Description: "messages that exhausted their retries, keyed by origin stream and consumer",
		Subjects:    []string{DeadLetterSubjectPrefix + ".>"},
		Retention:   nats.LimitsPolicy,
		MaxMsgs:     -1,
		MaxBytes:    maxBytes,
		MaxAge:      maxAge,
		Discard:     nats.DiscardOld,
		Storage:     nats.FileStorage,
		Replicas:    replicas,
		Duplicates:  2 * time.Minute,
	}
}

// DeadLetterSubject is the subject dead letters of one consumer are stored under.
func DeadLetterSubject(stream, consumer string) string {
	return fmt.Sprintf("%s.%s.%s", DeadLetterSubjectPrefix, stream, consumer)
}

// PublishDeadLetter stores message in the dead-letter stream. It keeps the
// original payload and headers (including trace context) and adds the failure
// details as Strongforce-Dlq-* headers.
func (nb *Broadcaster) PublishDeadLetter(ctx context.Context, message bus.InboundMessage, cause error) error {
	delivery := message.Delivery

	// Stable per origin message, so a dead letter re-published after a failed
	// Term is deduplicated by the stream.
	msgId := fmt.Sprintf("%s:%s:%d", delivery.Stream, delivery.Consumer, delivery.StreamSequence)

	ctx, cancel := context.WithTimeout(ctx, deadLetterPublishTimeout)
	defer cancel()

	_, err := nb.jetStream.PublishMsg(&nats.Msg{
		Subject: DeadLetterSubject(delivery.Stream, delivery.Consumer),
		Header:  deadLetterHeaders(message, cause),
		Data:    message.Data,
	}, nats.MsgId(msgId), nats.Context(ctx))

	return err
}

// deadLetterHeaders copies the message's headers and adds the failure details.
// Every Nats-* header is left out, including ones a producer set itself: they
// are JetStream directives such as Nats-Msg-Id or Nats-Expected-Stream that
// would make the server deduplicate or reject the dead letter.
func deadLetterHeaders(message bus.InboundMessage, cause error) nats.Header {
	delivery := message.Delivery

	headers := nats.Header{}
	for key, values := range message.Headers {
		if strings.HasPrefix(key, "Nats-") {
			continue
		}
		headers[key] = append([]string(nil), values...)
	}

	causeText := "unknown"
	if cause != nil {
		causeText = cause.Error()
	}

	headers.Set(DeadLetterHeaderError, sanitizeHeaderValue(causeText))
	headers.Set(DeadLetterHeaderStream, delivery.Stream)
	headers.Set(DeadLetterHeaderConsumer, delivery.Consumer)
	headers.Set(DeadLetterHeaderSubject, message.Subject)
	headers.Set(DeadLetterHeaderMessageId, message.Id)
	headers.Set(DeadLetterHeaderStreamSequence, strconv.FormatUint(delivery.StreamSequence, 10))
	headers.Set(DeadLetterHeaderNumDelivered, strconv.FormatUint(delivery.NumDelivered, 10))
	headers.Set(DeadLetterHeaderFailedAt, time.Now().UTC().Format(time.RFC3339Nano))

	return headers
}

// sanitizeHeaderValue strips line breaks, which would corrupt the header
// block, and truncates long values such as panic stack traces.
func sanitizeHeaderValue(value string) string {
	value = strings.NewReplacer("\r\n", " | ", "\n", " | ", "\r", " ").Replace(value)
	if len(value) > maxDeadLetterErrorLength {
		// Cut on a rune boundary so the header stays valid UTF-8.
		cut := maxDeadLetterErrorLength
		for cut > 0 && !utf8.RuneStart(value[cut]) {
			cut--
		}
		value = value[:cut]
	}
	return value
}
