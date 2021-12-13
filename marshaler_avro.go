package gluon

import (
	"errors"
	"io/ioutil"
	"path/filepath"

	"github.com/hamba/avro"
)

var ErrMissingAvroSchemaDefinition = errors.New("gluon: Missing Apache Avro schema definition")

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
	schema, err := m.getInternalSchemaDefinition(v)
	if err != nil {
		return nil, err
	}
	return avro.Marshal(schema, v)
}

func (m *MarshalerAvro) Unmarshal(data []byte, v interface{}) error {
	schema, err := m.getInternalSchemaDefinition(v)
	if err != nil {
		return err
	}
	return avro.Unmarshal(schema, data, v)
}

// uses lazy loading to avoid Bus cold boot exec time from increasing
//
// caches Avro schemas on internal Bus schema registry
func (m *MarshalerAvro) getInternalSchemaDefinition(v interface{}) (schemaAvro avro.Schema, err error) {
	meta, err := m.parentBus.schemaRegistry.get(v)
	if err != nil {
		return nil, err
	} else if meta.schemaInternalDefinition != nil {
		schema, ok := meta.schemaInternalDefinition.(avro.Schema)
		if ok {
			return schema, nil
		}
	}
	defer func() {
		if schemaAvro != nil {
			meta.schemaInternalDefinition = schemaAvro
		}
	}()

	if meta.SchemaURI == "" {
		return nil, ErrMissingAvroSchemaDefinition
	}

	// Note: If remote schema is selected (URI starts with http or https),
	// then make a http call to the remote schema registry with Avro JSON files
	schemaBuf, err := ioutil.ReadFile(filepath.Clean(meta.SchemaURI))
	if err != nil {
		return nil, err
	}

	schemaAvro, err = avro.Parse(string(schemaBuf))
	return
}
