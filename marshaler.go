package gluon

import "encoding/json"

// Marshaler component which converts streaming data into an specific type (e.g. Apache Avro, JSON)
type Marshaler interface {
	// ContentType retrieves the marshaler codec type (e.g. application/json)
	ContentType() string
	// Marshal parses the given data into an specific codec
	Marshal(data interface{}) (interface{}, error)
	// Unmarshal parses the given data into the reference object using the marshal's codec
	Unmarshal(data, obj interface{}) error
}

type JSONMarshaler struct {
}

var _ Marshaler = JSONMarshaler{}

func (m JSONMarshaler) ContentType() string {
	return "application/json"
}

func (m JSONMarshaler) Marshal(data interface{}) (interface{}, error) {
	return json.Marshal(data)
}

func (m JSONMarshaler) Unmarshal(data, obj interface{}) error {
	return json.Unmarshal(data.([]byte), obj)
}
