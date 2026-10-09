package bus

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// unhandledDeliveryFactor bounds how often a batch message may be delivered
// before it is dead-lettered without being handled. A batch subscription
// releases the messages it holds on every handover and each release counts
// as a delivery, so the bound is a multiple of the attempts; it still catches
// a message that crashes its subscriber every time.
const unhandledDeliveryFactor = 3

// wrapBatch applies the subscription's middleware around a batch handler. The
// middleware sees one message per batch that describes the batch.
func (s *Subscription) wrapBatch(route *BatchRoute) BatchHandlerFunc {
	return func(ctx context.Context, messages []InboundMessage) []error {
		var outcomes []error
		call := s.wrap(func(ctx context.Context, _ InboundMessage) error {
			outcomes = route.Handler(ctx, messages)
			return nil
		})

		err := invokeHandler(ctx, call, InboundMessage{
			MessageCtx: ctx,
			Subject:    route.Pattern,
			Delivery:   DeliveryInfo{Stream: s.stream, Consumer: s.consumer},
		})
		if err == nil && len(outcomes) != len(messages) {
			err = fmt.Errorf("%w: got %d for %d messages", ErrBatchOutcomes, len(outcomes), len(messages))
		}
		if err != nil {
			outcomes = make([]error, len(messages))
			for i := range outcomes {
				outcomes[i] = err
			}
		}
		return outcomes
	}
}

// runBatchWorker collects inbound messages into batches and hands them to the
// batch handler one at a time. A batch is due once it holds Size messages or
// Wait has passed since its first message. Messages that arrive while a batch
// runs are kept for the next one; every message the worker holds is kept from
// redelivery with InProgress.
func (s *Subscription) runBatchWorker(ctx context.Context) {
	defer s.workers.Done()

	var (
		pending   []InboundMessage
		running   []InboundMessage
		done      chan []error
		deadline  <-chan time.Time
		stalled   bool
		window    *time.Timer
		windowC   <-chan time.Time
		windowDue bool
		stopping  = s.stopping
		draining  bool
	)

	heartbeat := time.NewTicker(s.heartbeatInterval)
	defer heartbeat.Stop()

	dispatch := func() {
		if running != nil || draining || len(pending) == 0 {
			return
		}
		if len(pending) < s.batch.Size && !windowDue {
			return
		}

		size := min(len(pending), s.batch.Size)
		running = pending[:size:size]
		pending = append([]InboundMessage(nil), pending[size:]...)

		if window != nil {
			window.Stop()
			window, windowC = nil, nil
		}
		windowDue = false
		if len(pending) > 0 {
			// The rest waits for its own window, which started when it arrived.
			windowDue = true
		}

		done = make(chan []error, 1)
		go func(batch []InboundMessage) {
			done <- s.handleBatch(ctx, batch)
		}(running)

		if s.batchTimeout > 0 {
			deadline = time.After(s.batchTimeout + s.heartbeatInterval)
		}
	}

	// stall gives up a batch whose handler ignored its timeout: its messages
	// are no longer kept from redelivery, and receiving stops and the pin is
	// released, so another subscriber takes over until the handler returns.
	stall := func() {
		stalled = true
		s.logger.Errorf("batch handler did not return after its %s timeout, releasing its %d messages until it does", s.batchTimeout, len(running))
		if s.unsubscribe != nil {
			s.unsubscribe()
		}
		s.release(pending)
		pending = nil
		if s.unpin != nil && s.IsPinned() {
			if err := s.unpin(context.WithoutCancel(ctx)); err != nil {
				s.reportError(fmt.Errorf("failed to unpin: %w", err))
			}
		}
	}

	finish := func() {
		s.isRunning.Store(false)
		s.unhandledMu.Lock()
		s.unhandled = append(s.unhandled, pending...)
		s.unhandledMu.Unlock()
	}

	for {
		select {
		case <-ctx.Done():
			if done != nil {
				s.settleBatch(context.WithoutCancel(ctx), running, <-done)
			}
			finish()
			s.releaseUnhandled()
			return

		case <-stopping:
			stopping = nil
			draining = true
			if running == nil {
				finish()
				return
			}

		case message := <-s.inboundMessages:
			message.deserializer = s.deserializer
			if message.MessageCtx == nil {
				message.MessageCtx = ctx
			}

			if stalled {
				s.release([]InboundMessage{message})
				continue
			}

			if !s.retryPolicy.IsUnlimited() && message.Delivery.NumDelivered > uint64(unhandledDeliveryFactor*s.retryPolicy.MaxAttempts) {
				s.exhaust(message.MessageCtx, message, ErrDeliveryLimitExceeded)
				continue
			}

			pending = append(pending, message)
			if window == nil && !windowDue {
				window = time.NewTimer(s.batch.Wait)
				windowC = window.C
			}
			dispatch()

		case <-windowC:
			window, windowC = nil, nil
			windowDue = true
			dispatch()

		case <-deadline:
			deadline = nil
			stall()

		case outcomes := <-done:
			s.settleBatch(ctx, running, outcomes)
			running, done, deadline = nil, nil, nil
			if draining {
				finish()
				return
			}
			if stalled {
				stalled = false
				if s.startReceiving != nil {
					s.startReceiving(ctx)
				}
			}
			dispatch()

		case <-heartbeat.C:
			if !stalled {
				s.keepInProgress(running)
			}
			s.keepInProgress(pending)
		}
	}
}

// handleBatch runs the batch handler, bounded by the batch timeout.
func (s *Subscription) handleBatch(ctx context.Context, batch []InboundMessage) []error {
	if s.batchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.batchTimeout)
		defer cancel()
	}

	start := time.Now()
	outcomes := s.batchHandler(ctx, batch)

	failed := false
	for _, err := range outcomes {
		if err != nil && !IsSkip(err) {
			failed = true
			break
		}
	}
	s.metrics.recordHandlerDuration(ctx, s.stream, s.consumer, time.Since(start), failed)

	return outcomes
}

// settleBatch settles every message of a batch by its outcome. Each distinct
// failure is reported once, not once per message.
func (s *Subscription) settleBatch(ctx context.Context, batch []InboundMessage, outcomes []error) {
	reported := make(map[string]bool)
	var skips []error

	for i, message := range batch {
		err := outcomes[i]
		switch {
		case err == nil:
			if s.ack(message) {
				s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeAcked)
			}
		case IsSkip(err):
			skips = append(skips, err)
			if s.ack(message) {
				s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeSkipped)
			}
		default:
			err = fmt.Errorf("%w: %w", ErrMessageHandlerFailed, err)
			if !reported[err.Error()] {
				reported[err.Error()] = true
				s.reportError(err)
			}
			s.retryOrExhaust(ctx, message, err)
		}
	}

	if len(skips) > 0 {
		s.logger.Infof("skipped %d of %d messages: %s", len(skips), len(batch), errors.Join(skips...))
	}
}

// keepInProgress resets the AckWait of messages the subscription holds.
func (s *Subscription) keepInProgress(messages []InboundMessage) {
	for _, message := range messages {
		if message.InProgress == nil {
			continue
		}
		if err := message.InProgress(); err != nil {
			s.logger.Warnf("failed to extend ack wait of message %s (%s): %s", message.Id, message.Subject, err)
		}
	}
}
