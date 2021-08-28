package glocal

import "sync"

type schedulerBuffer struct {
	mu                 sync.Mutex
	notificationStream chan string
}

func newSchedulerBuffer() *schedulerBuffer {
	return &schedulerBuffer{
		mu:                 sync.Mutex{},
		notificationStream: make(chan string),
	}
}

func (s *schedulerBuffer) notify(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notificationStream <- topic
}

func (s *schedulerBuffer) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.notificationStream)
}
