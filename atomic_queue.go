package gluon

import (
	"sync"
)

type atomicQueue struct {
	queue  []interface{}
	length int

	mu sync.RWMutex
}

func newAtomicQueue() *atomicQueue {
	return &atomicQueue{
		queue:  make([]interface{}, 0),
		length: 0,
		mu:     sync.RWMutex{},
	}
}

func (q *atomicQueue) push(i interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, i)
	q.length++
}

func (q *atomicQueue) get(i int) interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if i > q.length {
		return nil
	}
	return q.queue[i]
}

func (q *atomicQueue) pop() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.length == 0 {
		return nil
	}

	i := q.queue[q.length-1]
	if q.length == 1 {
		q.queue = make([]interface{}, 0)
		q.length--
		return i
	}

	q.queue = q.queue[:q.length-1]
	q.length--
	return i
}
