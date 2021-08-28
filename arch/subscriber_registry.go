package arch

import (
	"sync"
)

// subscriberRegistry is a concurrent-safe internal agent used to manage subscriber entries.
type subscriberRegistry struct {
	mu               sync.RWMutex
	totalSubscribers uint // avoids using len(registry) to gain performance

	registry map[string][]*Subscriber
}

func newSubscriberRegistry() *subscriberRegistry {
	return &subscriberRegistry{
		mu:               sync.RWMutex{},
		totalSubscribers: 0,
		registry:         map[string][]*Subscriber{},
	}
}

func (r *subscriberRegistry) register(topic string, entry *Subscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var entries []*Subscriber
	var ok bool
	entries, ok = r.registry[topic]
	if !ok {
		entries = make([]*Subscriber, 0)
	}
	r.registry[topic] = append(entries, entry)
	r.totalSubscribers++
}

func (r *subscriberRegistry) get(topic string) []*Subscriber {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registry[topic]
}
