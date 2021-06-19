package gmemory

import (
	"context"
	"sync"
	"time"

	"github.com/neutrinocorp/gluon"
)

// bus is the Gluon in-memory Event Bus artifact implementation.
type bus struct {
	baseCtx        context.Context
	messageChan    chan *gluon.Message // actual stream of messages
	workerRegistry sync.Map
	broker         *gluon.Broker
}

func newBus() *bus {
	return &bus{
		baseCtx:        context.Background(),
		messageChan:    make(chan *gluon.Message),
		workerRegistry: sync.Map{},
	}
}

func (b *bus) register(w *worker, h *gluon.MessageHandler) {
	if _, ok := b.workerRegistry.Load(h.GetTopic()); !ok {
		b.workerRegistry.Store(h.GetTopic(), []*worker{})
	}
	val, _ := b.workerRegistry.Load(h.GetTopic())
	registry := val.([]*worker)
	registry = append(registry, w)
	b.workerRegistry.Store(h.GetTopic(), registry)
}

func (b *bus) start() {
	dlq := gluon.NewAtomicQueue() // Dead-Letter Queue
	go b.listenMessageStream(dlq)
	go b.reEnqueueFromDLQ(dlq) // handle failed messages while Broker is bootstrapping
}

func (b *bus) listenMessageStream(dlq *gluon.AtomicQueue) {
	for msg := range b.messageChan {
		if b.broker == nil || !b.broker.IsReady {
			dlq.Push(msg)
			time.Sleep(time.Millisecond * 500)
			continue
		}

		if entry, ok := b.workerRegistry.Load(msg.Type); ok {
			workers := entry.([]*worker)
			for _, w := range workers {
				w.messageChan <- msg
			}
		}
	}
}

func (b *bus) reEnqueueFromDLQ(dlq *gluon.AtomicQueue) {
	for {
		if dlq.Length == 0 {
			time.Sleep(time.Millisecond * 500)
			continue
		}

		for i := 0; i < dlq.Length; i++ {
			msg := dlq.Pop().(*gluon.Message)
			b.messageChan <- msg // re-enqueue
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (b *bus) publish(msg *gluon.Message) error {
	b.messageChan <- msg
	return nil
}

func (b *bus) close() {
	close(b.messageChan)
}
