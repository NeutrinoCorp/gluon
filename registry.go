package gluon

import (
	"errors"
	"sync"
)

type Registry struct {
	mu      sync.RWMutex
	entries map[string][]*Entry
}

var (
	ErrEntryAlreadyExists = errors.New("entry already exists")
	ErrEntryNotFound      = errors.New("entry not found")
)

func NewRegistry() *Registry {
	return &Registry{
		mu:      sync.RWMutex{},
		entries: map[string][]*Entry{},
	}
}

func (e *Registry) Event(event IntegrationEvent) *Entry {
	return e.register(event.Type, new(Entry))
}

func (e *Registry) Topic(topic string) *Entry {
	return e.register(topic, new(Entry))
}

func (e *Registry) register(topic string, entry *Entry) *Entry {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry.topic = topic
	e.entries[topic] = append(e.entries[topic], entry)
	return entry
}

func (e *Registry) Register(entry *Entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.entries[entry.topic]; ok {
		return ErrEntryAlreadyExists
	}

	e.entries[entry.topic] = append(e.entries[entry.topic], entry)
	return nil
}

func (e *Registry) Remove(topic string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.entries[topic]; !ok {
		return ErrEntryNotFound
	}

	delete(e.entries, topic)
	return nil
}

func (e *Registry) List(topic string) ([]*Entry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	entry, ok := e.entries[topic]
	if !ok {
		return nil, ErrEntryNotFound
	}
	return entry, nil
}
