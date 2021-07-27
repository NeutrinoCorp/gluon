package gluon

import "encoding/json"

// Marshaller component which converts streaming data into an specific type (e.g. Apache Avro, JSON)
type Marshaller interface {
	// ContentType retrieves the marshaller codec type (e.g. application/json)
	ContentType() string
	// Marshal parses the given data into an specific codec
	Marshal(data interface{}) (interface{}, error)
	// Unmarshal parses the given data into the reference object using the marshal's codec
	Unmarshal(data, obj interface{}) error
}

// JSONMarshaller handles JSON codec parsing of messages
type JSONMarshaller struct {
}

var _ Marshaller = JSONMarshaller{}

func (m JSONMarshaller) ContentType() string {
	return "application/json"
}

func (m JSONMarshaller) Marshal(data interface{}) (interface{}, error) {
	return json.Marshal(data)
}

func (m JSONMarshaller) Unmarshal(data, obj interface{}) error {
	return json.Unmarshal(data.([]byte), obj)
}
