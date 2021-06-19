package gluon

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Broker is a component which manages all message-driven operations of the current system.
//
// To start subscribing to messages, it is required to do that through a broker.
//
// In addition, it contains configurations and default values for specific Gluon operations.
type Broker struct {
	Registry    *Registry
	BaseContext context.Context

	scheduler      *scheduler
	doneChan       chan struct{}
	mu             sync.Mutex
	isShuttingDown atomicBool
	driver         Driver
}

var (
	shutdownPollInterval = time.Millisecond * 500
)

var (
	// ErrBrokerClosed the given broker has been closed and cannot execute the given operation
	ErrBrokerClosed = errors.New("gluon: Broker is closed")
)

// NewBroker allocates a new broker
func NewBroker() *Broker {
	return &Broker{
		Registry:       NewRegistry(),
		mu:             sync.Mutex{},
		doneChan:       make(chan struct{}),
		isShuttingDown: 0,
		driver:         DefaultDriver,
	}
}

// Topic sets a new message handler using the given parameter as key (aka. topic)
func (b *Broker) Topic(t string) *MessageHandler {
	return b.Registry.Topic(t)
}

// Message sets a new message handler using properties of the given message
func (b *Broker) Message(m Message) *MessageHandler {
	return b.Registry.Message(m)
}

// ListenAndServe starts subscription tasks concurrently safely
func (b *Broker) ListenAndServe() error {
	if b.shuttingDown() {
		return ErrBrokerClosed
	}
	return b.Serve()
}

// Serve starts subscription tasks concurrently
func (b *Broker) Serve() error {
	for {
		if b.BaseContext == nil {
			b.BaseContext = context.Background()
		}
		b.startScheduler(b.BaseContext)

		<-b.getDoneChanLocked()
		b.Shutdown(b.BaseContext)
	}
}

// starts the task scheduler component
func (b *Broker) startScheduler(ctx context.Context) {
	b.scheduler = newScheduler(b)
	for _, entries := range b.Registry.entries {
		b.scheduler.ScheduleJobs(ctx, entries)
	}
}

// Shutdown triggers the given broker graceful shutdown
func (b *Broker) Shutdown(ctx context.Context) error {
	b.isShuttingDown.setTrue()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closeDoneChanLocked()
	go b.Registry.close()

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()

	for {
		errChan := make(chan error)
		go func() {
			b.shutdownScheduler(ctx, errChan)
		}()

		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Broker) shutdownScheduler(ctx context.Context, errChan chan<- error) {
	b.scheduler.Shutdown(ctx, errChan)
}

func (b *Broker) shuttingDown() bool {
	return b.isShuttingDown.isSet()
}

func (b *Broker) getDoneChanLocked() chan struct{} {
	if b.doneChan == nil {
		b.doneChan = make(chan struct{})
	}
	return b.doneChan
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
