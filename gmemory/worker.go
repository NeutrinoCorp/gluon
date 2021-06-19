package gmemory

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/neutrinocorp/gluon"
)

type worker struct {
	broker *gluon.Broker
}

func newWorker(b *gluon.Broker) *worker {
	return &worker{
		broker: b,
	}
}

var _ gluon.Worker = &worker{}

func (w *worker) Execute(ctx context.Context, h *gluon.MessageHandler) {
	go func() {
		for {
			h.GetSubscriberFunc()(ctx, gluon.Message{
				Type: h.GetTopic(),
			})
			time.Sleep(time.Minute * 1)
		}
	}()
}

func (w *worker) Close(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error) {
	log.Print("closing worker")
	// errChan <- errors.New("generic handler error")
	wg.Done()
}
