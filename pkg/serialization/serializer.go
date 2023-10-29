package serialization

type Serializer interface {
	Serialize(input interface{}) ([]byte, error)
	Deserialize(input []byte, dst interface{}) error
}
