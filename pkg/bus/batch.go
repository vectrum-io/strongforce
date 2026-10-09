package bus

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultBatchSize is the most messages a batch handler receives at once.
	DefaultBatchSize = 100
	// DefaultBatchWait is how long a batch collects messages after its first
	// one arrived.
	DefaultBatchWait = time.Second
)

var ErrInvalidBatch = errors.New("invalid batch subscription")

// ErrBatchOutcomes is the outcome of every message of a batch whose handler
// returned a different number of outcomes than it received messages.
var ErrBatchOutcomes = errors.New("batch handler returned the wrong number of outcomes")

// BatchHandlerFunc handles a batch of messages and returns one outcome per
// message, in the order of messages: nil acks it, Skip acks it as skipped,
// Permanent dead-letters it and any other error retries it.
type BatchHandlerFunc func(ctx context.Context, messages []InboundMessage) []error

// Message is a decoded message of a batch.
type Message[T any] struct {
	InboundMessage
	Event *T
}

// BatchRoute is the handler declared with HandleBatch.
type BatchRoute struct {
	Pattern string
	Handler BatchHandlerFunc
	// Size is the most messages per batch. It is also the consumer's
	// MaxAckPending, so a subscription never holds more than one batch.
	Size int
	// Wait is how long a batch collects messages after its first one.
	Wait time.Duration
}

type BatchOption func(*BatchRoute)

// BatchSize sets the most messages one call of the handler receives.
func BatchSize(n int) BatchOption {
	return func(route *BatchRoute) {
		route.Size = n
	}
}

// BatchWait sets how long a batch collects messages after its first one
// arrived before the handler is called with what is there.
func BatchWait(d time.Duration) BatchOption {
	return func(route *BatchRoute) {
		route.Wait = d
	}
}

// HandleBatch declares a handler that receives the messages matching pattern
// in batches. Batches are handled one at a time; while a batch runs, its
// messages are kept from redelivery with InProgress, and messages that arrive
// meanwhile wait for the next batch. Each payload is decoded into a new T; a
// payload that cannot be decoded is Permanent and not passed to fn.
//
// A subscription with a batch handler declares no other handler. Middleware
// runs once per batch, with a message that carries the batch's stream,
// consumer and pattern but no payload.
func HandleBatch[T any](pattern string, fn func(ctx context.Context, messages []Message[T]) []error, opts ...BatchOption) SubscribeOption {
	return HandleBatchRaw(pattern, func(ctx context.Context, messages []InboundMessage) []error {
		outcomes := make([]error, len(messages))
		decoded := make([]Message[T], 0, len(messages))
		positions := make([]int, 0, len(messages))

		for i, message := range messages {
			event := new(T)
			if err := message.Unmarshal(event); err != nil {
				outcomes[i] = fmt.Errorf("failed to decode %s: %w", message.Subject, err)
				continue
			}
			decoded = append(decoded, Message[T]{InboundMessage: message, Event: event})
			positions = append(positions, i)
		}

		if len(decoded) == 0 {
			return outcomes
		}

		handled := fn(ctx, decoded)
		if len(handled) != len(decoded) {
			err := fmt.Errorf("%w: got %d for %d messages", ErrBatchOutcomes, len(handled), len(decoded))
			for _, i := range positions {
				outcomes[i] = err
			}
			return outcomes
		}

		for j, i := range positions {
			outcomes[i] = handled[j]
		}
		return outcomes
	}, opts...)
}

// HandleBatchRaw declares a batch handler that decodes the payloads itself.
// See HandleBatch.
func HandleBatchRaw(pattern string, fn BatchHandlerFunc, opts ...BatchOption) SubscribeOption {
	return func(options *SubscriptionOptions) {
		route := &BatchRoute{
			Pattern: pattern,
			Handler: fn,
			Size:    DefaultBatchSize,
			Wait:    DefaultBatchWait,
		}
		for _, opt := range opts {
			opt(route)
		}
		options.Batch = route
	}
}

// ValidateBatch checks the batch handler of a subscription.
func (o SubscriptionOptions) ValidateBatch() error {
	if o.Batch == nil {
		if o.PinnedGroup != nil {
			return fmt.Errorf("%w: a pinned priority group needs a batch handler", ErrInvalidBatch)
		}
		return nil
	}
	if len(o.Routes) > 0 || len(o.FilterSubjects) > 0 {
		return fmt.Errorf("%w: a batch handler cannot be combined with other handlers or filter subjects", ErrInvalidBatch)
	}
	if err := ValidatePattern(o.Batch.Pattern); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBatch, err)
	}
	if o.Batch.Handler == nil {
		return fmt.Errorf("%w: no handler for %s", ErrInvalidBatch, o.Batch.Pattern)
	}
	if o.Batch.Size < 1 {
		return fmt.Errorf("%w: batch size must be positive", ErrInvalidBatch)
	}
	if o.Batch.Wait <= 0 {
		return fmt.Errorf("%w: batch wait must be positive", ErrInvalidBatch)
	}
	if o.PinnedGroup != nil {
		if o.PinnedGroup.Group == "" {
			return fmt.Errorf("%w: pinned priority group needs a name", ErrInvalidBatch)
		}
		if o.PinnedGroup.TTL <= 0 {
			return fmt.Errorf("%w: pinned TTL must be positive", ErrInvalidBatch)
		}
	}
	return nil
}
