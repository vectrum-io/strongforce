package bus

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/vectrum-io/strongforce/pkg/serialization"
)

var (
	ErrHandlerRegistrationFailed = errors.New("failed to register handler")
	ErrMessageNotRoutable        = errors.New("message is not routable")
	ErrMessageHandlerFailed      = errors.New("message could not be handled")
)

type HandlerFunc func(ctx context.Context, message InboundMessage) error
type ErrorCallbackFunc func(err error)
type UnsubscribeFn func()

type Subscription struct {
	unsubscribe     UnsubscribeFn
	inboundMessages chan InboundMessage
	handlers        map[string]HandlerFunc
	handlersMu      sync.RWMutex
	onError         ErrorCallbackFunc
	deserializer    serialization.Serializer
	isRunning       bool
	concurrency     int
}

// NewSubscription builds a subscription that dispatches inbound messages to
// concurrency goroutines. concurrency<=0 collapses to 1 — the historical
// single-goroutine behaviour callers used to get implicitly.
func NewSubscription(inboundMessages chan InboundMessage, concurrency int, deserializer serialization.Serializer, unsubscribe UnsubscribeFn) *Subscription {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Subscription{
		unsubscribe:     unsubscribe,
		handlers:        make(map[string]HandlerFunc),
		inboundMessages: inboundMessages,
		handlersMu:      sync.RWMutex{},
		deserializer:    deserializer,
		concurrency:     concurrency,
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
	isMessageRouted := false
	var handlerErrors []error

	message.deserializer = s.deserializer

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
		if s.onError != nil {
			s.onError(fmt.Errorf("%w: %s", ErrMessageNotRoutable, message.Subject))
		}
		return
	}

	if len(handlerErrors) > 0 {
		if s.onError != nil {
			s.onError(fmt.Errorf("%w: %w", ErrMessageHandlerFailed, errors.Join(handlerErrors...)))
		}
		return
	}

	if err := message.Ack(); err != nil {
		if s.onError != nil {
			s.onError(fmt.Errorf("%w: failed to ack message: %w", ErrMessageHandlerFailed, err))
		}
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
