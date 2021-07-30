package gmemory

import (
	"context"
	"log"
	"reflect"
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

func (w *worker) Execute(ctx context.Context, wg *sync.WaitGroup, c *gluon.Consumer) {
	w.driver.bus.register(w, c)
	defer wg.Done()
	go func() {
		for msg := range w.messageChan {
			if msg.Type != c.GetTopic() {
				continue
			}

			if w.broker.Config.Marshaller != nil && w.broker.Config.Marshaller.ContentType() == msg.DataContentType {
				dataType := reflect.TypeOf(c.GetSubscribedMessage())
				dataRef := reflect.New(dataType)
				err := w.broker.Config.Marshaller.Unmarshal(msg.Data, &dataRef)
				log.Println(err)
				msg.Data = dataRef
			}
			if subFunc := c.GetSubscriberFunc(); subFunc != nil {
				go subFunc(ctx, *msg)
			}
			if sub := c.GetSubscriber(); sub != nil {
				go sub.Handle(ctx, *msg)
			}
		}
	}()
}

func (w *worker) Close(_ context.Context, wg *sync.WaitGroup, _ chan<- error) {
	close(w.messageChan)
	wg.Done()
}
