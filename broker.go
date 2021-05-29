package gluon

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Broker struct {
	Registry    *Registry
	Publisher   Publisher
	BaseContext context.Context

	scheduler  *scheduler
	doneChan   chan struct{}
	errChan    chan error
	mu         sync.Mutex
	inShutdown atomicBool
}

var (
	shutdownPollInterval = time.Millisecond * 500
)

var (
	ErrBrokerClosed = errors.New("broker is closed")
)

func NewBroker() *Broker {
	return &Broker{
		Registry:   NewRegistry(),
		mu:         sync.Mutex{},
		inShutdown: 0,
	}
}

func (b *Broker) Topic(t string) *Entry {
	return b.Registry.Topic(t)
}

func (b *Broker) Event(e IntegrationEvent) *Entry {
	return b.Registry.Event(e)
}

// ListenAndServe starts listening to the given Consumer(s) concurrently-safe
func (b *Broker) ListenAndServe() error {
	if b.shuttingDown() {
		return ErrBrokerClosed
	}
	return b.Serve()
}

// Serve starts the broker components
func (b *Broker) Serve() error {
	if b.BaseContext == nil {
		b.BaseContext = context.Background()
	}

	b.startScheduler(b.BaseContext)

	go func() {
		for err := range b.getErrChanLocked() {
			if err != nil {
				b.getDoneChanLocked() <- struct{}{} // stop broker
			}
		}
	}()

	<-b.getDoneChanLocked()
	return b.Shutdown(b.BaseContext)
}

func (b *Broker) startScheduler(ctx context.Context) {
	b.scheduler = newScheduler(b)
	for _, entries := range b.Registry.entries {
		b.scheduler.ScheduleJobs(ctx, entries, b.getErrChanLocked())
	}
}

// Shutdown starts Broker graceful shutdown of its components
func (b *Broker) Shutdown(ctx context.Context) error {
	b.inShutdown.setTrue()
	defer b.inShutdown.setFalse()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closeDoneChanLocked()
	b.closeErrChanLocked()

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()
	for {
		if err := b.shutdownScheduler(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Broker) shutdownScheduler(ctx context.Context) error {
	return b.scheduler.Shutdown(ctx)
}

func (b *Broker) shuttingDown() bool {
	return b.inShutdown.isSet()
}

func (b *Broker) getDoneChanLocked() chan struct{} {
	if b.doneChan == nil {
		b.doneChan = make(chan struct{})
	}
	return b.doneChan
}

func (b *Broker) getErrChanLocked() chan error {
	if b.errChan == nil {
		b.errChan = make(chan error)
	}
	return b.errChan
}

func (b *Broker) closeDoneChanLocked() {
	ch := b.getDoneChanLocked()
	select {
	case <-ch:
		// Already closed. Don't close again.
	default:
		// Safe to close here. We're the only closer, guarded
		// by s.mu.
		close(ch)
	}
}

func (b *Broker) closeErrChanLocked() {
	ch := b.getErrChanLocked()
	select {
	case <-ch:
		// Already closed. Don't close again.
	default:
		// Safe to close here. We're the only closer, guarded
		// by s.mu.
		close(ch)
	}
}
