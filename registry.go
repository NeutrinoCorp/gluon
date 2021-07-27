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
	entries map[string][]*Consumer
}

var (
	// ErrTopicAlreadyExists the given topic is already present on the Gluon registry
	ErrTopicAlreadyExists = errors.New("gluon: Topic already exists")
	// ErrTopicNotFound the given topic is not present on the Gluon registry
	ErrTopicNotFound = errors.New("gluon: Topic not found")
)

// NewRegistry allocates a new message registry.
func NewRegistry() *Registry {
	return &Registry{
		mu:      sync.RWMutex{},
		entries: map[string][]*Consumer{},
	}
}

// Message sets a new message consumer using properties of the given message
func (r *Registry) Message(msg Message) *Consumer {
	return r.register(msg.Type, new(Consumer))
}

// Topic sets a new message consumer using the given parameter as key (aka. topic)
func (r *Registry) Topic(topic string) *Consumer {
	return r.register(topic, new(Consumer))
}

// sets an entry using the given topic as key and the given consumer
func (r *Registry) register(topic string, c *Consumer) *Consumer {
	r.mu.Lock()
	defer r.mu.Unlock()
	c.topic = topic
	r.entries[topic] = append(r.entries[topic], c)
	return c
}

// Register sets an entry from a message consumer properties
func (r *Registry) Register(c *Consumer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[c.topic]; ok {
		return ErrTopicAlreadyExists
	}

	r.entries[c.topic] = append(r.entries[c.topic], c)
	return nil
}

// Remove deletes an entry from the registry
func (r *Registry) Remove(topic string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[topic]; !ok {
		return ErrTopicNotFound
	}

	delete(r.entries, topic)
	return nil
}

// List retrieves all handlers from the given topic
func (r *Registry) List(topic string) ([]*Consumer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c, ok := r.entries[topic]
	if !ok {
		return nil, ErrTopicNotFound
	}
	return c, nil
}

// Exists indicates if the given topic record exists within the registry
func (r *Registry) Exists(topic string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.entries[topic]
	return ok
}

// Close clear memory allocation
func (r *Registry) close() {
	for k := range r.entries {
		delete(r.entries, k) // clean memory allocs
	}
}
