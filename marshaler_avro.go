package gluon

import (
	"github.com/hamba/avro"
)

type MarshalerAvro struct {
	parentBus *Bus
}

func NewMarshalerAvro() *MarshalerAvro {
	return &MarshalerAvro{}
}

var _ Marshaler = &MarshalerAvro{}

func (m *MarshalerAvro) SetParentBus(b *Bus) {
	m.parentBus = b
}

func (m *MarshalerAvro) GetContentType() string {
	return "application/avro"
}

func (m *MarshalerAvro) Marshal(v interface{}) ([]byte, error) {
	schema, err := m.getAvroInternalSchemaDefinition(v)
	if err != nil {
		return nil, err
	}
	return avro.Marshal(schema, v)
}

func (m *MarshalerAvro) Unmarshal(data []byte, v interface{}) error {
	schema, err := m.getAvroInternalSchemaDefinition(v)
	if err != nil {
		return err
	}
	return avro.Unmarshal(schema, data, v)
}

// uses lazy loading to avoid Bus cold boot exec time from increasing
//
// caches Avro schemas on internal Bus schema registry
func (m *MarshalerAvro) getAvroInternalSchemaDefinition(v interface{}) (schemaAvro avro.Schema, err error) {
	meta, err := m.parentBus.internalSchemaRegistry.get(v)
	if err != nil {
		return nil, err
	}

	schemaDef, err := m.parentBus.SchemaRegistry.GetSchemaDefinition(meta.SchemaName, meta.SchemaVersion)
	if err != nil {
		return nil, err
	}

	schemaAvro, err = avro.Parse(schemaDef)
	return
}
