package forwarder

import "context"

type Forwarder interface {
	Stop() error
	Start(ctx context.Context) error
}
