package glocal

import "sync"

type partitionRegistry struct {
	mu              sync.RWMutex
	topicPartitions map[string]*partition // Key: partition_index#topic_name
}

func newPartitionRegistry() *partitionRegistry {
	return &partitionRegistry{
		mu:              sync.RWMutex{},
		topicPartitions: map[string]*partition{},
	}
}

func (l *partitionRegistry) set(k string, p *partition) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.topicPartitions[k] = p
}

func (l *partitionRegistry) get(k string) *partition {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.topicPartitions[k]
}
