package gluon

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

type worker struct {
	broker *Broker
}

func newWorker(b *Broker) *worker {
	return &worker{
		broker: b,
	}
}

func (w *worker) Execute(ctx context.Context, e *Entry, errChan chan<- error) {
	go func() {
		count := 2
		for {
			log.Printf("subscribing to %s from group %s", e.topic, e.group)
			time.Sleep(time.Second * 3)
			if count == 0 && e.topic == "foo-event" {
				errChan <- errors.New("generic error")
				break
			}
			count--
		}
	}()
}

func (w *worker) Close(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error) {
	log.Print("closing worker")
	errChan <- errors.New("error while closing")
	wg.Done()
}
