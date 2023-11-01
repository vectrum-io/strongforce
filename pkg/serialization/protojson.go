package serialization

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ProtoJSON struct{}

func NewProtoJSONSerializer() *ProtoJSON {
	return &ProtoJSON{}
}

func (p *ProtoJSON) Serialize(input interface{}) ([]byte, error) {
	pb, ok := input.(proto.Message)
	if !ok {
		return nil, ErrNoProtobufPayload
	}

	return protojson.Marshal(pb)
}

func (p *ProtoJSON) Deserialize(input []byte, dst interface{}) error {
	pb, ok := dst.(proto.Message)
	if !ok {
		return ErrNoProtobufDestination
	}

	return protojson.Unmarshal(input, pb)
}
