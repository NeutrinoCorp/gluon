package gmemory

import (
	"context"
	"sync"

	"github.com/neutrinocorp/gluon"
)

type worker struct {
	broker      *gluon.Broker
	driver      *Driver
	messageChan chan *gluon.Message
}

func newWorker(b *gluon.Broker, d *Driver) *worker {
	return &worker{
		broker:      b,
		driver:      d,
		messageChan: make(chan *gluon.Message),
	}
}

var _ gluon.Worker = &worker{}

func (w *worker) Execute(ctx context.Context, wg *sync.WaitGroup, h *gluon.MessageHandler) {
	w.driver.bus.register(w, h)
	defer wg.Done()
	go func() {
		for msg := range w.messageChan {
			if msg.Type != h.GetTopic() {
				continue
			}

			if subFunc := h.GetSubscriberFunc(); subFunc != nil {
				go subFunc(ctx, *msg)
			}
			if sub := h.GetSubscriber(); sub != nil {
				go sub.Handle(ctx, *msg)
			}
		}
	}()
}

func (w *worker) Close(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error) {
	close(w.messageChan)
	wg.Done()
}
