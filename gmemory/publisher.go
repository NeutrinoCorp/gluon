package gmemory

import (
	"context"

	"github.com/neutrinocorp/gluon"
)

type MemoryPublisher struct {
	bus *bus
}

// NewMemoryPublisher allocates a new publisher for in-memory operations
func NewMemoryPublisher() *MemoryPublisher {
	return defaultPublisher
}

var _ gluon.Publisher = MemoryPublisher{}

func (p MemoryPublisher) PublishMessage(ctx context.Context, msg *gluon.Message) error {
	go p.bus.publish(msg)
	return nil
}
