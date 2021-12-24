package gluon

import (
	"github.com/hamba/avro"
)

type MarshalerAvro struct{}

func NewMarshalerAvro() *MarshalerAvro {
	return &MarshalerAvro{}
}

var _ Marshaler = &MarshalerAvro{}

func (m *MarshalerAvro) GetContentType() string {
	return "application/avro"
}

func (m *MarshalerAvro) Marshal(schemaDef string, v interface{}) ([]byte, error) {
	schemaAvro, err := avro.Parse(schemaDef)
	if err != nil {
		return nil, err
	}
	return avro.Marshal(schemaAvro, v)
}

func (m *MarshalerAvro) Unmarshal(schemaDef string, data []byte, v interface{}) error {
	schemaAvro, err := avro.Parse(schemaDef)
	if err != nil {
		return err
	}
	return avro.Unmarshal(schemaAvro, data, v)
}
