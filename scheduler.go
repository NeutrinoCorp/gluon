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
	workerQueue *AtomicQueue
}

func newScheduler(b *Broker) *scheduler {
	return &scheduler{
		broker: b,
		workerPool: sync.Pool{
			New: func() interface{} {
				return b.driver.NewWorker(b)
			},
		},
		workerQueue: NewAtomicQueue(),
	}
}

func (s *scheduler) ScheduleJobs(ctx context.Context, wg *sync.WaitGroup, consumers []*Consumer) {
	wg.Add(len(consumers))
	for _, h := range consumers {
		w := s.workerPool.New().(Worker)
		s.workerQueue.Push(w)
		go w.Execute(ctx, wg, h)
	}
}

func (s *scheduler) Shutdown(ctx context.Context, errChan chan<- error) {
	errs := &multierror.Error{}
	errSchedulerChan := make(chan error)
	defer s.cleanMemoryResources()
	defer close(errChan)
	go s.aggregateErrorStream(errSchedulerChan, errs)

	wg := &sync.WaitGroup{}
	for i := 0; i < s.workerQueue.Length; i++ {
		wg.Add(1)
		w := s.workerQueue.Get(i).(Worker)
		go w.Close(ctx, wg, errSchedulerChan) // start greceful shutdown in parallel
	}
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
	for i := 0; i < s.workerQueue.Length; i++ {
		w := s.workerQueue.Pop().(Worker)
		s.workerPool.Put(w)
	}
}
