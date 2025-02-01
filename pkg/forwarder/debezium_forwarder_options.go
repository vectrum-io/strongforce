package forwarder

import (
	"fmt"
	"github.com/vectrum-io/strongforce/pkg/serialization"
	"go.uber.org/zap"
)

type DebeziumOptions struct {
	Serializer      serialization.Serializer
	DebeziumStream  string
	DebeziumSubject string
	SubscriberName  string
	Logger          *zap.Logger
}

func (o *DebeziumOptions) validate() error {
	if o.DebeziumStream == "" {
		return fmt.Errorf("debezium stream is required")
	}

	if o.DebeziumSubject == "" {
		return fmt.Errorf("debezium subject is required")
	}

	if o.SubscriberName == "" {
		return fmt.Errorf("subscriber name is required")
	}

	if o.Serializer == nil {
		o.Serializer = DefaultOptions.Serializer
	}

	if o.Logger == nil {
		o.Logger = zap.L()
	}

	return nil
}
