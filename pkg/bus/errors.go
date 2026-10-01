package bus

import (
	"errors"
	"fmt"
)

// ErrDeliveryLimitExceeded is the dead-letter cause for a message that arrives
// after its last attempt, e.g. because its handler kept exceeding AckWait.
var ErrDeliveryLimitExceeded = errors.New("delivery limit exceeded")

type permanentError struct {
	err error
}

// Permanent marks a handler error as non-retryable: the message skips its
// remaining attempts and is dead-lettered right away. The mark survives
// wrapping with %w.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

func (e *permanentError) Error() string {
	return e.err.Error()
}

func (e *permanentError) Unwrap() error {
	return e.err
}

// IsPermanent reports whether err or any error it wraps was marked Permanent.
func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}

type skipError struct {
	reason string
}

// Skip acks a message without handling it, e.g. because the entity it refers
// to was deleted in the meantime. The reason is logged and the message is
// counted as skipped.
func Skip(format string, args ...any) error {
	return &skipError{reason: fmt.Sprintf(format, args...)}
}

func (e *skipError) Error() string {
	return "skipped: " + e.reason
}

// IsSkip reports whether err or any error it wraps was created by Skip.
func IsSkip(err error) bool {
	var se *skipError
	return errors.As(err, &se)
}
