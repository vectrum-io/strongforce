package bus

import (
	"context"
	"errors"
	"fmt"
)

var ErrInvalidRoutes = errors.New("invalid subscription routes")

// Route binds a subject pattern to the handler for the messages matching it.
type Route struct {
	Pattern string
	Handler HandlerFunc
}

// Middleware wraps every handler of a subscription, e.g. to prepare the
// handler context. The first middleware of a list is the outermost one.
type Middleware func(next HandlerFunc) HandlerFunc

// Handle declares the handler for messages matching pattern. The payload is
// decoded into a new T before fn runs; a payload that cannot be decoded is
// Permanent. The pattern also becomes a filter subject of the consumer.
func Handle[T any](pattern string, fn func(ctx context.Context, event *T, message InboundMessage) error) SubscribeOption {
	return HandleRaw(pattern, func(ctx context.Context, message InboundMessage) error {
		event := new(T)
		if err := message.Unmarshal(event); err != nil {
			return fmt.Errorf("failed to decode %s: %w", message.Subject, err)
		}
		return fn(ctx, event, message)
	})
}

// HandleRaw declares a handler that decodes the payload itself. The pattern
// also becomes a filter subject of the consumer.
func HandleRaw(pattern string, fn HandlerFunc) SubscribeOption {
	return func(options *SubscriptionOptions) {
		options.Routes = append(options.Routes, Route{Pattern: pattern, Handler: fn})
	}
}

// ValidateRoutes checks that routes are well-formed and returns their
// patterns, which are the filter subjects of the consumer serving them.
func ValidateRoutes(routes []Route) ([]string, error) {
	patterns := make([]string, 0, len(routes))
	seen := make(map[string]struct{}, len(routes))

	for _, route := range routes {
		if err := ValidatePattern(route.Pattern); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidRoutes, err)
		}
		if route.Handler == nil {
			return nil, fmt.Errorf("%w: no handler for %s", ErrInvalidRoutes, route.Pattern)
		}
		if _, ok := seen[route.Pattern]; ok {
			return nil, fmt.Errorf("%w: %s is declared twice", ErrInvalidRoutes, route.Pattern)
		}
		seen[route.Pattern] = struct{}{}
		patterns = append(patterns, route.Pattern)
	}

	return patterns, nil
}
