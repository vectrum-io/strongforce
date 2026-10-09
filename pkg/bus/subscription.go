package bus

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
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
	// NoRedelivery marks messages the broker cannot redeliver, e.g. core NATS
	// broadcasts: failed messages are reported and dropped instead of retried.
	NoRedelivery bool
	// Routes are registered up front and fix the handler set: AddHandler is
	// rejected afterwards. They must have passed ValidateRoutes.
	Routes []Route
	// Middleware wraps every handler, the first one outermost.
	Middleware []Middleware
	// HandlerTimeout bounds how long the handlers of one message may run.
	// Zero means no limit.
	HandlerTimeout time.Duration
	// Batch dispatches messages in batches to its handler instead of Routes.
	// It must have passed SubscriptionOptions.ValidateBatch.
	Batch *BatchRoute
	// HeartbeatInterval is how often the messages a batch subscription holds
	// are kept from redelivery with InProgress.
	HeartbeatInterval time.Duration
	// BatchTimeout bounds how long one batch may run. Zero means no limit.
	BatchTimeout time.Duration
	// Pinned reports whether the broker currently delivers to this
	// subscriber; nil means the subscription has no pinned group.
	Pinned func() bool
	// Unpin releases the pin during Shutdown so another subscriber takes over
	// right away.
	Unpin func(ctx context.Context) error
	// StartReceiving is called by Start, for brokers that deliver only once
	// the subscription runs.
	StartReceiving func(ctx context.Context)
}

type Subscription struct {
	unsubscribe     UnsubscribeFn
	inboundMessages chan InboundMessage
	handlers        map[string]HandlerFunc
	handlersMu      sync.RWMutex
	onError         ErrorCallbackFunc
	deserializer    serialization.Serializer
	isRunning       atomic.Bool
	concurrency     int
	retryPolicy     RetryPolicy
	deadLetter      DeadLetterFunc
	metrics         *Metrics
	logger          *zap.SugaredLogger
	stream          string
	consumer        string
	noRedelivery    bool
	middleware      []Middleware
	handlerTimeout  time.Duration
	declaredRoutes  bool

	batch             *BatchRoute
	batchHandler      BatchHandlerFunc
	heartbeatInterval time.Duration
	batchTimeout      time.Duration
	pinned            func() bool
	unpin             func(ctx context.Context) error
	startReceiving    func(ctx context.Context)

	// stopping is closed by Shutdown; workers finish their current work and
	// return.
	stopping    chan struct{}
	stopOnce    sync.Once
	workers     sync.WaitGroup
	cancelWork  context.CancelFunc
	unhandledMu sync.Mutex
	unhandled   []InboundMessage
	startedMu   sync.Mutex
	started     bool
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
	s := &Subscription{
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
		stream:         settings.Stream,
		consumer:       settings.Consumer,
		noRedelivery:   settings.NoRedelivery,
		middleware:     settings.Middleware,
		handlerTimeout: settings.HandlerTimeout,

		batch:             settings.Batch,
		heartbeatInterval: settings.HeartbeatInterval,
		batchTimeout:      settings.BatchTimeout,
		pinned:            settings.Pinned,
		unpin:             settings.Unpin,
		startReceiving:    settings.StartReceiving,
		stopping:          make(chan struct{}),
	}

	if s.batch != nil {
		s.concurrency = 1
		s.batchHandler = s.wrapBatch(s.batch)
	}

	for _, route := range settings.Routes {
		s.handlers[route.Pattern] = s.wrap(route.Handler)
	}
	s.declaredRoutes = len(settings.Routes) > 0

	return s
}

// wrap applies the subscription's middleware to fn, the first one outermost.
func (s *Subscription) wrap(fn HandlerFunc) HandlerFunc {
	for i := len(s.middleware) - 1; i >= 0; i-- {
		fn = s.middleware[i](fn)
	}
	return fn
}

func (s *Subscription) Stop() {
	if s.unsubscribe != nil {
		s.unsubscribe()
	}
}

func (s *Subscription) IsRunning() bool {
	return s.isRunning.Load()
}

// IsPinned reports whether this subscriber currently holds the pin of its
// priority group. It is false for subscriptions without a pinned group.
func (s *Subscription) IsPinned() bool {
	return s.pinned != nil && s.pinned()
}

// Shutdown stops the subscription gracefully: no new messages are handled,
// the messages being handled are settled, the ones received but not yet
// handled are released for immediate redelivery, and a held pin is given up.
// When ctx ends first, running handlers are cancelled and Shutdown waits for
// them to return.
func (s *Subscription) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopping) })

	if s.batch == nil {
		// Without batching nothing is buffered between deliveries, so the
		// consumer can stop before the workers finish.
		s.Stop()
	}

	workersDone := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
	case <-ctx.Done():
		s.startedMu.Lock()
		if s.cancelWork != nil {
			s.cancelWork()
		}
		s.startedMu.Unlock()
		<-workersDone
	}

	if s.batch != nil {
		s.Stop()
	}

	s.releaseUnhandled()

	if s.unpin != nil && s.IsPinned() {
		if err := s.unpin(context.WithoutCancel(ctx)); err != nil {
			return fmt.Errorf("failed to unpin: %w", err)
		}
	}

	return nil
}

// releaseUnhandled naks messages that were received but not handled, so the
// broker redelivers them right away instead of after AckWait.
func (s *Subscription) releaseUnhandled() {
	s.unhandledMu.Lock()
	unhandled := s.unhandled
	s.unhandled = nil
	s.unhandledMu.Unlock()

	for {
		select {
		case message := <-s.inboundMessages:
			unhandled = append(unhandled, message)
			continue
		default:
		}
		break
	}

	for _, message := range unhandled {
		if message.Nak == nil {
			continue
		}
		if err := message.Nak(0); err != nil {
			s.reportError(fmt.Errorf("%w: failed to release message: %w", ErrMessageHandlerFailed, err))
		}
	}
}

func (s *Subscription) OnError(errorFunc ErrorCallbackFunc) {
	s.onError = errorFunc
}

func (s *Subscription) RemoveHandler(pattern string) {
	s.handlersMu.Lock()
	delete(s.handlers, pattern)
	s.handlersMu.Unlock()
}

// AddHandler registers a handler for messages matching pattern. Subscriptions
// whose handlers were declared with Handle or HandleRaw reject it, since their
// filter subjects cannot change anymore.
func (s *Subscription) AddHandler(pattern string, handlerFunc HandlerFunc) error {
	if s.declaredRoutes {
		return fmt.Errorf("%w: handlers of this subscription are declared with Handle", ErrHandlerRegistrationFailed)
	}

	if err := ValidatePattern(pattern); err != nil {
		return fmt.Errorf("failed to validate pattern: %w", err)
	}

	s.handlersMu.Lock()
	defer s.handlersMu.Unlock()

	_, ok := s.handlers[pattern]
	if ok {
		return fmt.Errorf("%w: handler already registered", ErrHandlerRegistrationFailed)
	}

	s.handlers[pattern] = s.wrap(handlerFunc)

	return nil
}

func (s *Subscription) Start(ctx context.Context) {
	s.startedMu.Lock()
	defer s.startedMu.Unlock()
	if s.started {
		return
	}
	select {
	case <-s.stopping:
		return
	default:
	}
	s.started = true
	s.isRunning.Store(true)

	ctx, s.cancelWork = context.WithCancel(ctx)

	if s.startReceiving != nil {
		s.startReceiving(ctx)
	}

	if s.batch != nil {
		s.workers.Add(1)
		go s.runBatchWorker(ctx)
		return
	}

	// Spawn concurrency workers all racing on the same inboundMessages channel.
	// Go's channel receive is the synchronisation point — each message goes to
	// exactly one worker. When ctx ends every worker observes Done on its next
	// iteration; isRunning flips on the first worker that returns.
	for i := 0; i < s.concurrency; i++ {
		s.workers.Add(1)
		go s.runWorker(ctx)
	}
}

func (s *Subscription) runWorker(ctx context.Context) {
	defer s.workers.Done()
	for {
		select {
		case <-ctx.Done():
			s.isRunning.Store(false)
			return
		case <-s.stopping:
			s.isRunning.Store(false)
			return
		case message := <-s.inboundMessages:
			s.handleMessage(message)
		}
	}
}

func (s *Subscription) handleMessage(message InboundMessage) {
	message.deserializer = s.deserializer
	if message.MessageCtx == nil {
		message.MessageCtx = context.Background()
	}
	ctx := message.MessageCtx

	// Only reachable when a handler kept exceeding AckWait, since failed
	// handlers are settled explicitly below.
	if !s.retryPolicy.IsUnlimited() && message.Delivery.NumDelivered > uint64(s.retryPolicy.MaxAttempts) {
		s.exhaust(ctx, message, ErrDeliveryLimitExceeded)
		return
	}

	isMessageRouted := false
	var handlerErrors, skips []error

	handlerCtx, cancel := s.handlerContext(ctx)
	defer cancel()
	start := time.Now()
	s.handlersMu.RLock()
	for pattern, fn := range s.handlers {
		if !MatchSubject(message.Subject, pattern) {
			continue
		}

		isMessageRouted = true

		err := invokeHandler(handlerCtx, fn, message)
		switch {
		case err == nil:
		case IsSkip(err):
			skips = append(skips, err)
		default:
			handlerErrors = append(handlerErrors, err)
		}
	}
	s.handlersMu.RUnlock()

	// A message without a matching handler is retried like a failed one, so
	// it is not lost while handlers are still being registered, and is
	// dead-lettered once its attempts are used up.
	if !isMessageRouted {
		s.settleFailure(ctx, message, fmt.Errorf("%w: %s", ErrMessageNotRoutable, message.Subject))
		return
	}

	s.metrics.recordHandlerDuration(ctx, s.stream, s.consumer, time.Since(start), len(handlerErrors) > 0)

	if len(handlerErrors) == 0 {
		outcome := OutcomeAcked
		if len(skips) > 0 {
			outcome = OutcomeSkipped
			s.logger.Infof("skipped message %s (%s): %s", message.Id, message.Subject, errors.Join(skips...))
		}
		if s.ack(message) {
			s.metrics.recordOutcome(ctx, s.stream, s.consumer, outcome)
		}
		return
	}

	s.settleFailure(ctx, message, fmt.Errorf("%w: %w", ErrMessageHandlerFailed, errors.Join(handlerErrors...)))
}

// handlerContext bounds the handlers of one message by the handler timeout.
// Settling uses the unbounded message context, so it still runs after the
// handlers timed out.
func (s *Subscription) handlerContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.handlerTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.handlerTimeout)
}

// settleFailure reports err and naks the message for a retry, or exhausts it
// when err is Permanent or the message has no attempts left.
func (s *Subscription) settleFailure(ctx context.Context, message InboundMessage, err error) {
	s.reportError(err)
	s.retryOrExhaust(ctx, message, err)
}

// retryOrExhaust naks a failed message for a retry, or exhausts it when err
// is Permanent or the message has no attempts left.
func (s *Subscription) retryOrExhaust(ctx context.Context, message InboundMessage, err error) {
	if s.noRedelivery {
		s.logger.Warnf("dropping message %s (%s) that cannot be redelivered: %s", message.Id, message.Subject, err)
		s.metrics.recordOutcome(ctx, s.stream, s.consumer, OutcomeDropped)
		return
	}

	if IsPermanent(err) || s.retryPolicy.IsExhausted(message.Delivery.NumDelivered) {
		s.exhaust(ctx, message, err)
		return
	}

	if nakErr := message.Nak(s.retryPolicy.Delay(message.Delivery.NumDelivered)); nakErr != nil {
		s.reportError(fmt.Errorf("%w: failed to nak message: %w", ErrMessageHandlerFailed, nakErr))
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
