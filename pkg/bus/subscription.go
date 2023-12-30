package bus

import (
	"context"
	"errors"
	"fmt"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"sync"
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
}

func NewSubscription(inboundMessages chan InboundMessage, deserializer serialization.Serializer, unsubscribe UnsubscribeFn) *Subscription {
	return &Subscription{
		unsubscribe:     unsubscribe,
		handlers:        make(map[string]HandlerFunc),
		inboundMessages: inboundMessages,
		handlersMu:      sync.RWMutex{},
		deserializer:    deserializer,
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

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.isRunning = false
				return
			case message := <-s.inboundMessages:
				isMessageRouted := false
				var handlerErrors []error

				// prepare message
				message.deserializer = s.deserializer

				s.handlersMu.RLock()
				for pattern, fn := range s.handlers {
					if !MatchSubject(message.Subject, pattern) {
						continue
					}

					isMessageRouted = true

					if err := fn(message.MessageCtx, message); err != nil {
						handlerErrors = append(handlerErrors, err)
					}
				}
				s.handlersMu.RUnlock()

				// could not route message
				if !isMessageRouted {
					if s.onError != nil {
						s.onError(fmt.Errorf("%w: %s", ErrMessageNotRoutable, message.Subject))
					}
					continue
				}

				// there were errors in the handler functions
				if len(handlerErrors) > 0 {
					if s.onError != nil {
						s.onError(fmt.Errorf("%w: %w", ErrMessageHandlerFailed, errors.Join(handlerErrors...)))
					}
					continue
				}

				if err := message.Ack(); err != nil {
					if s.onError != nil {
						s.onError(fmt.Errorf("%w: failed to ack message: %w", ErrMessageHandlerFailed, err))
					}
					continue
				}
			}
		}
	}()
}
