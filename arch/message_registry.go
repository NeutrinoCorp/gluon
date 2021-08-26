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
}

// messageRegistry Is a concurrent-safe internal agent which relations message concrete types to useful metadata.
//
// The metadata is composed by the message's topic name, source, data schema URI and subject. Most of the previous
// fields come from the CloudEvents specification.
//
// It is widely recommended to use specific types rather than use primitives as this agent uses the message type
// to make the relation to the specified metadata.
//
// For more information about most of the metadata fields, check: https://github.com/cloudevents/spec/blob/master/spec.md
type messageRegistry struct {
	mu       sync.RWMutex
	registry map[string]*MessageMetadata // Key: struct type, Val: topic
}

func newMessageRegistry() *messageRegistry {
	return &messageRegistry{
		mu:       sync.RWMutex{},
		registry: map[string]*MessageMetadata{},
	}
}

func (r *messageRegistry) register(meta MessageMetadata, msg interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	typeStr := reflect.TypeOf(msg).String()
	if _, ok := r.registry[typeStr]; ok {
		return
	}
	r.registry[typeStr] = &meta
}

func (r *messageRegistry) get(msg interface{}) (MessageMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	typeStr := reflect.TypeOf(msg).String()
	if meta, ok := r.registry[typeStr]; ok {
		return *meta, nil
	}
	return MessageMetadata{}, ErrMessageNotRegistered
}
