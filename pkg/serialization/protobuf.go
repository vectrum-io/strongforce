package serialization

import (
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNoProtobufPayload     = errors.New("payload is not protobuf serializable")
	ErrNoProtobufDestination = errors.New("destination is not protobuf deserializable")
)

type Protobuf struct{}

func NewProtobufSerializer() *Protobuf {
	return &Protobuf{}
}

func (p *Protobuf) Serialize(input interface{}) ([]byte, error) {
	pb, ok := input.(proto.Message)
	if !ok {
		return nil, ErrNoProtobufPayload
	}

	return proto.Marshal(pb)
}

func (p *Protobuf) Deserialize(input []byte, dst interface{}) error {
	pb, ok := dst.(proto.Message)
	if !ok {
		return ErrNoProtobufDestination
	}

	return proto.Unmarshal(input, pb)
}
