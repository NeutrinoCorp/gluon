package glocal

import (
	"sync"

	"github.com/neutrinocorp/gluon/arch"
)

type partition struct {
	mu            sync.RWMutex
	totalMessages int
	queue         []arch.TransportMessage
	lastMessage   arch.TransportMessage
	offsets       map[string]int // Key: consumer_group, Val: offset index
}

func newPartition() *partition {
	return &partition{
		mu:            sync.RWMutex{},
		totalMessages: 0,
		queue:         []arch.TransportMessage{},
		offsets:       map[string]int{},
	}
}

func (p *partition) push(msg *arch.TransportMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queue = append(p.queue, *msg)
	p.lastMessage = *msg
	p.totalMessages++
}
