package gluon

import (
	"errors"
	"sync"
)

// Registry is the Gluon component in charge of mapping message keys (aka. topics) with their respective handlers.
//
// A message might have up to N handlers.
type Registry struct {
	mu      sync.RWMutex
	entries map[string][]*MessageHandler
}

var (
	// ErrTopicAlreadyExists the given topic is already present on the given registry
	ErrTopicAlreadyExists = errors.New("topic already exists")
	// ErrTopicNotFound the given topic is not present on the given registry
	ErrTopicNotFound = errors.New("topic not found")
)

// NewRegistry allocates a new message registry.
func NewRegistry() *Registry {
	return &Registry{
		mu:      sync.RWMutex{},
		entries: map[string][]*MessageHandler{},
	}
}

// Message sets a new message handler using properties of the given message
func (e *Registry) Message(msg Message) *MessageHandler {
	return e.register(msg.Type, new(MessageHandler))
}

// Topic sets a new message handler using the given parameter as key (aka. topic)
func (e *Registry) Topic(topic string) *MessageHandler {
	return e.register(topic, new(MessageHandler))
}

// sets an entry using the given topic as key and the given handler
func (e *Registry) register(topic string, handler *MessageHandler) *MessageHandler {
	e.mu.Lock()
	defer e.mu.Unlock()
	handler.topic = topic
	e.entries[topic] = append(e.entries[topic], handler)
	return handler
}

// Register sets an entry from a message handler properties
func (e *Registry) Register(handler *MessageHandler) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.entries[handler.topic]; ok {
		return ErrTopicAlreadyExists
	}

	e.entries[handler.topic] = append(e.entries[handler.topic], handler)
	return nil
}

// Remove deletes an entry from the registry
func (e *Registry) Remove(topic string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.entries[topic]; !ok {
		return ErrTopicNotFound
	}

	delete(e.entries, topic)
	return nil
}

// List retrieves all handlers from the given topic
func (e *Registry) List(topic string) ([]*MessageHandler, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	MessageHandler, ok := e.entries[topic]
	if !ok {
		return nil, ErrTopicNotFound
	}
	return MessageHandler, nil
}

// Close clear memory allocation
func (e *Registry) Close() {
	for k := range e.entries {
		delete(e.entries, k) // clean memory allocs
	}
}
