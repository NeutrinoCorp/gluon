package gluon

import (
	"sync"
)

// AtomicQueue concurrent safe FIFO queue
type AtomicQueue struct {
	queue  []interface{}
	Length int

	mu sync.RWMutex
}

// NewAtomicQueue allocates a new FIFO queue
func NewAtomicQueue() *AtomicQueue {
	return &AtomicQueue{
		queue:  make([]interface{}, 0),
		Length: 0,
		mu:     sync.RWMutex{},
	}
}

// Push add a value to the queue's tail
func (q *AtomicQueue) Push(i interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, i)
	q.Length++
}

// Get retrieves an specific value from the queue using the given index without removing it
func (q *AtomicQueue) Get(i int) interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if i > q.Length {
		return nil
	}
	return q.queue[i]
}

// Pop retrieves and removes the next value in the queue
func (q *AtomicQueue) Pop() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.Length == 0 {
		return nil
	}

	i := q.queue[q.Length-1]
	if q.Length == 1 {
		q.queue = make([]interface{}, 0)
		q.Length--
		return i
	}

	q.queue = q.queue[:q.Length-1]
	q.Length--
	return i
}
