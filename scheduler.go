package gluon

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
)

// scheduler is a tasks scheduler for subscribing jobs
type scheduler struct {
	broker      *Broker
	workerPool  sync.Pool
	workerQueue *atomicQueue
}

func newScheduler(b *Broker) *scheduler {
	return &scheduler{
		broker: b,
		workerPool: sync.Pool{
			New: func() interface{} {
				return b.WorkerFactory.New(b)
			},
		},
		workerQueue: newAtomicQueue(),
	}
}

func (s *scheduler) ScheduleJobs(ctx context.Context, handlers []*MessageHandler) {
	for _, h := range handlers {
		w := s.workerPool.New().(Worker)
		s.workerQueue.push(w)
		go w.Execute(ctx, h)
	}
}

func (s *scheduler) Shutdown(ctx context.Context, errChan chan<- error) {
	errs := &multierror.Error{}
	errSchedulerChan := make(chan error)
	defer s.cleanMemoryResources()
	defer close(errChan)
	wg := &sync.WaitGroup{}
	for i := 0; i < s.workerQueue.length; i++ {
		wg.Add(1)
		w := s.workerQueue.get(i).(Worker)
		go w.Close(ctx, wg, errSchedulerChan) // start greceful shutdown in parallel
	}
	go s.aggregateErrorStream(errSchedulerChan, errs)
	wg.Wait()
	errChan <- errs.ErrorOrNil()
}

func (s *scheduler) aggregateErrorStream(errChan <-chan error, errs *multierror.Error) {
	for err := range errChan {
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}
}

func (s *scheduler) cleanMemoryResources() {
	for i := 0; i < s.workerQueue.length; i++ {
		w := s.workerQueue.pop().(Worker)
		s.workerPool.Put(w)
	}
}
