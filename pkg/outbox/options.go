package outbox

import "github.com/vectrum-io/strongforce/pkg/serialization"

const DefaultOutboxTableName = "event_outbox"

type Options struct {
	TableName  string
	Serializer serialization.Serializer
}

func (o *Options) validate() error {
	if o.TableName == "" {
		o.TableName = DefaultOutboxTableName
	}

	if o.Serializer == nil {
		o.Serializer = serialization.NewProtobufSerializer()
	}

	return nil
}
