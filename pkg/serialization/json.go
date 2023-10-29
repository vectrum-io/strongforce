package serialization

import (
	"encoding/json"
)

type JSON struct{}

func NewJSONSerializer() *JSON {
	return &JSON{}
}

func (p *JSON) Serialize(input interface{}) ([]byte, error) {
	return json.Marshal(input)
}

func (p *JSON) Deserialize(input []byte, dst interface{}) error {
	return json.Unmarshal(input, dst)
}
