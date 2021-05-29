package gluon

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
)

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
				return newWorker(b)
			},
		},
		workerQueue: newAtomicQueue(),
	}
}

func (s *scheduler) ScheduleJobs(ctx context.Context, entries []*Entry, errChan chan<- error) {
	for _, e := range entries {
		w := s.workerPool.New().(*worker)
		s.workerQueue.push(w)
		go w.Execute(ctx, e, errChan)
	}
}

func (s *scheduler) Shutdown(ctx context.Context) error {
	errs := &multierror.Error{}
	errChan := make(chan error) // local err stream, DO NOT use Broker err stream as it triggers shutdown
	wg := &sync.WaitGroup{}
	for i := 0; i < s.workerQueue.length; i++ {
		wg.Add(1)
		w := s.workerQueue.get(i).(*worker)
		go w.Close(ctx, wg, errChan) // start greceful shutdown in parallel
	}
	go func() {
		for err := range errChan {
			errs = multierror.Append(err, errs)
		}
	}()
	wg.Wait()
	s.cleanMemoryResources()
	close(errChan)
	return errs.ErrorOrNil()
}

func (s *scheduler) cleanMemoryResources() {
	for i := 0; i < s.workerQueue.length; i++ {
		w := s.workerQueue.pop().(*worker)
		s.workerPool.Put(w)
	}
}
