package arch

import (
	"errors"
	"reflect"
	"sync"
)

var (
	// ErrMessageNotRegistered The message type was not found on the message registry
	ErrMessageNotRegistered = errors.New("gluon: The specified message type is not present on the message registry")
)

type MessageMetadata struct {
	Topic         string
	Source        string
	SchemaURI     string
	SchemaVersion int

	schemaInternalType reflect.Type
}

// schemaRegistry Is a concurrent-safe internal agent which relations message concrete types to useful metadata.
//
// The metadata is composed by the message's topic name, source, data schema URI and subject. Most of the previous
// fields come from the CloudEvents specification.
//
// It is widely recommended to use specific types rather than use primitives as this agent uses the message type
// to make the relation to the specified metadata.
//
// For more information about most of the metadata fields, check: https://github.com/cloudevents/spec/blob/master/spec.md
type schemaRegistry struct {
	mu       sync.RWMutex
	registry map[string]*MessageMetadata // Key: struct type, Val: topic
}

func newSchemaRegistry() *schemaRegistry {
	return &schemaRegistry{
		mu:       sync.RWMutex{},
		registry: map[string]*MessageMetadata{},
	}
}

func (r *schemaRegistry) register(schema interface{}, meta MessageMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	schemaType := reflect.TypeOf(schema)
	if _, ok := r.registry[schemaType.String()]; ok {
		return
	}
	meta.schemaInternalType = schemaType
	r.registry[schemaType.String()] = &meta
}

func (r *schemaRegistry) get(schema interface{}) (MessageMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	typeStr := reflect.TypeOf(schema).String()
	if meta, ok := r.registry[typeStr]; ok {
		return *meta, nil
	}
	return MessageMetadata{}, ErrMessageNotRegistered
}

func (r *schemaRegistry) getByTopic(t string) *MessageMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, v := range r.registry {
		if v.Topic == t {
			return v
		}
	}
	return nil
}
