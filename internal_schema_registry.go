package gluon

import (
	"errors"
	"reflect"
	"strings"
	"sync"
)

var (
	// ErrMessageNotRegistered The message type was not found on the message registry
	ErrMessageNotRegistered = errors.New("gluon: The specified message type is not present on the message registry")
)

// MessageMetadata Is a set of definitions to describe a specific message schema.
type MessageMetadata struct {
	Topic         string
	Source        string
	SchemaName    string
	SchemaVersion int

	SchemaInternalType reflect.Type
}

// internalSchemaRegistry Is a concurrent-safe internal database which relations message concrete types to useful metadata.
//
// The metadata is composed by the message's topic name, source, data schema URI and subject. Most of the previous
// fields come from the CloudEvents specification.
//
// It is widely recommended to use specific types rather than use primitives as this agent uses the message type
// to make the relation to the specified metadata.
//
// For more information about most of the metadata fields, check: https://github.com/cloudevents/spec/blob/master/spec.md
type internalSchemaRegistry struct {
	mu       sync.RWMutex
	registry map[string]*MessageMetadata // Key: struct type, Val: topic
}

func newInternalSchemaRegistry() *internalSchemaRegistry {
	return &internalSchemaRegistry{
		mu:       sync.RWMutex{},
		registry: map[string]*MessageMetadata{},
	}
}

func (r *internalSchemaRegistry) register(schema interface{}, meta MessageMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	schemaType := reflect.TypeOf(schema)
	if _, ok := r.registry[schemaType.String()]; ok {
		return
	}
	meta.SchemaInternalType = schemaType
	r.registry[schemaType.String()] = &meta
}

func (r *internalSchemaRegistry) get(schema interface{}) (*MessageMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	typeStr := reflect.TypeOf(schema).String()
	// Note: remove references to a native type
	// this helps marshalers which depend on the internal schema registry as they receive pointers when decoding
	typeStr = strings.Replace(typeStr, "*", "", 1)
	if meta, ok := r.registry[typeStr]; ok {
		return meta, nil
	}
	return nil, ErrMessageNotRegistered
}

func (r *internalSchemaRegistry) getByKey(k string) (*MessageMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if meta, ok := r.registry[k]; ok {
		return meta, nil
	}
	return nil, ErrMessageNotRegistered
}

func (r *internalSchemaRegistry) getByTopic(t string) *MessageMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, v := range r.registry {
		if v.Topic == t {
			return v
		}
	}
	return nil
}
