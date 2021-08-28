package glocal

import (
	"github.com/neutrinocorp/gluon/arch"
)

type partition struct {
	totalMessages int
	lastMessage   arch.TransportMessage
}

func newPartition() *partition {
	return &partition{
		totalMessages: 0,
	}
}

func (p *partition) push(msg *arch.TransportMessage) {
	p.lastMessage = *msg
	p.totalMessages++
}
