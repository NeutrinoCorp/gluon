package glocal

import "sync"

type scheduler struct {
	mu                 sync.Mutex
	notificationStream chan string
}

func newScheduler() *scheduler {
	return &scheduler{
		mu:                 sync.Mutex{},
		notificationStream: make(chan string),
	}
}

func (s *scheduler) notify(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notificationStream <- topic
}

func (s *scheduler) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.notificationStream)
}
