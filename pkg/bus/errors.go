package bus

import "errors"

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
