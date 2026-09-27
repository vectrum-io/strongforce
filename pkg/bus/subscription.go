package bus

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.uber.org/zap"
)

var (
	ErrHandlerRegistrationFailed = errors.New("failed to register handler")
	ErrMessageNotRoutable        = errors.New("message is not routable")
	ErrMessageHandlerFailed      = errors.New("message could not be handled")
)

type HandlerFunc func(ctx context.Context, message InboundMessage) error
type ErrorCallbackFunc func(err error)
type UnsubscribeFn func()

// DeadLetterFunc parks a message that will not be retried anymore, together
// with the error that made it fail.
type DeadLetterFunc func(ctx context.Context, message InboundMessage, cause error) error

// SubscriptionSettings configures how a Subscription dispatches and settles
// inbound messages.
type SubscriptionSettings struct {
	// Concurrency is the number of handler goroutines; <=0 means 1.
	Concurrency  int
	Deserializer serialization.Serializer
	Unsubscribe  UnsubscribeFn
	RetryPolicy  RetryPolicy
	// DeadLetter receives exhausted messages. When nil they are dropped.
	DeadLetter DeadLetterFunc
	Metrics    *Metrics
	Logger     *zap.Logger
	// Stream and Consumer label logs and metrics.
	Stream   string
	Consumer string
}

type Subscription struct {
	unsubscribe     UnsubscribeFn
	inboundMessages chan InboundMessage
	handlers        map[string]HandlerFunc
	handlersMu      sync.RWMutex
	onError         ErrorCallbackFunc
	deserializer    serialization.Serializer
	isRunning       bool
	concurrency     int
	retryPolicy     RetryPolicy
	deadLetter      DeadLetterFunc
	metrics         *Metrics
	logger          *zap.SugaredLogger
	stream          string
	consumer        string
}

// NewSubscription builds a subscription that dispatches inbound messages to
// concurrency goroutines with the default retry policy and no dead-lettering.
func NewSubscription(inboundMessages chan InboundMessage, concurrency int, deserializer serialization.Serializer, unsubscribe UnsubscribeFn) *Subscription {
	return NewSubscriptionWithSettings(inboundMessages, SubscriptionSettings{
		Concurrency:  concurrency,
		Deserializer: deserializer,
		Unsubscribe:  unsubscribe,
		RetryPolicy:  DefaultRetryPolicy,
	})
}

func NewSubscriptionWithSettings(inboundMessages chan InboundMessage, settings SubscriptionSettings) *Subscription {
	if settings.Concurrency <= 0 {
		settings.Concurrency = 1
	}
	if settings.Logger == nil {
		settings.Logger = zap.L()
	}
	return &Subscription{
		unsubscribe:     settings.Unsubscribe,
		handlers:        make(map[string]HandlerFunc),
		inboundMessages: inboundMessages,
		deserializer:    settings.Deserializer,
		concurrency:     settings.Concurrency,
		retryPolicy:     settings.RetryPolicy.normalize(),
		deadLetter:      settings.DeadLetter,
		metrics:         settings.Metrics,
		logger: settings.Logger.Sugar().With(
			"stream", settings.Stream,
			"consumer", settings.Consumer,
		),
		stream:   settings.Stream,
		consumer: settings.Consumer,
	}
}

func (s *Subscription) Stop() {
	if s.unsubscribe != nil {
		s.unsubscribe()
	}
}

func (s *Subscription) IsRunning() bool {
	return s.isRunning
}

func (s *Subscription) OnError(errorFunc ErrorCallbackFunc) {
	s.onError = errorFunc
}

func (s *Subscription) RemoveHandler(pattern string) {
	s.handlersMu.Lock()
	delete(s.handlers, pattern)
	s.handlersMu.Unlock()
}

func (s *Subscription) AddHandler(pattern string, handlerFunc HandlerFunc) error {
	if err := ValidatePattern(pattern); err != nil {
		return fmt.Errorf("failed to validate pattern: %w", err)
	}

	s.handlersMu.Lock()
	defer s.handlersMu.Unlock()

	_, ok := s.handlers[pattern]
	if ok {
		return fmt.Errorf("%w: handler already registered", ErrHandlerRegistrationFailed)
	}

	s.handlers[pattern] = handlerFunc

	return nil
}

func (s *Subscription) Start(ctx context.Context) {
	s.isRunning = true

	// Spawn concurrency workers all racing on the same inboundMessages channel.
	// Go's channel receive is the synchronisation point — each message goes to
	// exactly one worker. When ctx ends every worker observes Done on its next
	// iteration; isRunning flips on the first worker that returns.
	for i := 0; i < s.concurrency; i++ {
		go s.runWorker(ctx)
	}
}

func (s *Subscription) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.isRunning = false
			return
		case message := <-s.inboundMessages:
			s.handleMessage(message)
		}
	}
}

func (s *Subscription) handleMessage(message InboundMessage) {
	message.deserializer = s.deserializer
	ctx := message.MessageCtx
	if ctx == nil {
		ctx = context.Background()
	}

	// Only reachable when a handler kept exceeding AckWait, since failed
	// handlers are settled explicitly below.
	if s.retryPolicy.MaxAttempts > 0 && message.Delivery.NumDelivered > uint64(s.retryPolicy.MaxAttempts) {
		s.exhaust(ctx, message, ErrDeliveryLimitExceeded)
		return
	}

	isMessageRouted := false
	var handlerErrors []error

	start := time.Now()
	s.handlersMu.RLock()
	for pattern, fn := range s.handlers {
		if !MatchSubject(message.Subject, pattern) {
			continue
		}

		isMessageRouted = true

		if err := invokeHandler(message.MessageCtx, fn, message); err != nil {
			handlerErrors = append(handlerErrors, err)
		}
	}
	s.handlersMu.RUnlock()

	if !isMessageRouted {
		s.logger.Warnf("acking message without a matching handler: %s", message.Subject)
		if s.ack(message) {
			s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeUnroutable)
		}
		return
	}

	s.metrics.recordHandlerDuration(ctx, s.stream, s.consumer, time.Since(start), len(handlerErrors) > 0)

	if len(handlerErrors) == 0 {
		if s.ack(message) {
			s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeAcked)
		}
		return
	}

	handlerErr := fmt.Errorf("%w: %w", ErrMessageHandlerFailed, errors.Join(handlerErrors...))
	s.reportError(handlerErr)

	if IsPermanent(handlerErr) || s.retryPolicy.IsExhausted(message.Delivery.NumDelivered) {
		s.exhaust(ctx, message, handlerErr)
		return
	}

	if err := message.Nak(s.retryPolicy.Delay(message.Delivery.NumDelivered)); err != nil {
		s.reportError(fmt.Errorf("%w: failed to nak message: %w", ErrMessageHandlerFailed, err))
	}
	s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeRetried)
}

func (s *Subscription) ack(message InboundMessage) bool {
	if err := message.Ack(); err != nil {
		s.reportError(fmt.Errorf("%w: failed to ack message: %w", ErrMessageHandlerFailed, err))
		return false
	}
	return true
}

// exhaust settles a message that will not be retried: it is dead-lettered and
// terminated, or dropped when the subscription has no dead letter.
func (s *Subscription) exhaust(ctx context.Context, message InboundMessage, cause error) {
	if s.deadLetter == nil {
		s.logger.Warnf("dropping message %s (%s) after %d deliveries: %s", message.Id, message.Subject, message.Delivery.NumDelivered, cause)
		s.terminate(message)
		s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeDropped)
		return
	}

	if err := s.deadLetter(ctx, message, cause); err != nil {
		s.reportError(fmt.Errorf("%w: failed to dead-letter message: %w", ErrMessageHandlerFailed, err))
		s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeDeadLetterFailed)
		if nakErr := message.Nak(s.retryPolicy.MaxDelay); nakErr != nil {
			s.reportError(fmt.Errorf("%w: failed to nak message: %w", ErrMessageHandlerFailed, nakErr))
		}
		return
	}

	s.logger.Errorf("dead-lettered message %s (%s) after %d deliveries: %s", message.Id, message.Subject, message.Delivery.NumDelivered, cause)
	s.terminate(message)
	s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeDeadLettered)
}

func (s *Subscription) terminate(message InboundMessage) {
	if message.Term == nil {
		return
	}
	if err := message.Term(); err != nil {
		s.reportError(fmt.Errorf("%w: failed to terminate message: %w", ErrMessageHandlerFailed, err))
	}
}

func (s *Subscription) reportError(err error) {
	if s.onError != nil {
		s.onError(err)
	}
}

// invokeHandler calls fn and converts a panic into an error.
func invokeHandler(ctx context.Context, fn HandlerFunc, message InboundMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v\n%s", r, debug.Stack())
		}
	}()

	return fn(ctx, message)
}
