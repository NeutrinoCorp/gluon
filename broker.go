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
	Publisher   Publisher
	BaseContext context.Context
	IsReady     bool
	Config      BrokerConfiguration

	scheduler      *scheduler
	doneChan       chan struct{}
	mu             sync.Mutex
	isShuttingDown atomicBool
	driver         Driver
}

const shutdownPollInterval = time.Millisecond * 500

var (
	// ErrBrokerClosed the given broker has been closed and cannot execute the given operation
	ErrBrokerClosed = errors.New("gluon: Broker is closed")
)

// NewBroker allocates a new broker
func NewBroker(driver string, opts ...Option) *Broker {
	options := newBrokerDefaultOptions(driver)
	for _, o := range opts {
		o.apply(&options)
	}

	b := &Broker{
		Registry:       NewRegistry(),
		Publisher:      options.publisher,
		mu:             sync.Mutex{},
		doneChan:       make(chan struct{}),
		isShuttingDown: 0,
		driver:         drivers[driver],
		IsReady:        false,
		Config: BrokerConfiguration{
			Group:     options.group,
			Source:    options.source,
			IDFactory: options.idFactory,
			Marshaler: options.marshaler,
			Networking: brokerNetworkConfig{
				Hosts: options.hosts,
			},
			Resiliency: brokerResiliencyConfig{
				MaxRetries:      options.maxRetries,
				MinRetryBackoff: options.minRetryBackoff,
				MaxRetryBackoff: options.maxRetryBackoff,
			},
			Behaviours: brokerBehaviours{},
		},
	}
	drivers[driver].SetBroker(b)
	return b
}

// sets required default values for broker's ops
func newBrokerDefaultOptions(driver string) options {
	return options{
		baseContext:     context.Background(),
		idFactory:       RandomIDFactory{},
		publisher:       drivers[driver],
		hosts:           make([]string, 0),
		maxRetries:      defaultMaxRetries,
		minRetryBackoff: defaultMinRetryBackoff,
		maxRetryBackoff: defaultMaxRetryBackoff,
	}
}

// Topic sets a new message consumer using the given parameter as key (aka. topic)
func (b *Broker) Topic(t string) *Consumer {
	return b.Registry.Topic(t)
}

// Message sets a new message consumer using properties of the given message
func (b *Broker) Message(m Message) *Consumer {
	return b.Registry.Message(m)
}

// Event sets a new event consumer using the given event topic
func (b *Broker) Event(e Event) *Consumer {
	return b.Registry.Topic(e.Topic())
}

// Publish propagates the given event to the whole system through the message broker
func (b *Broker) Publish(ctx context.Context, e Event) error {
	id, err := b.Config.IDFactory.NewID()
	if err != nil {
		return err
	}

	var data interface{} = e
	contentType := ""
	if b.Config.Marshaler != nil {
		contentType = b.Config.Marshaler.ContentType()
		data, err = b.Config.Marshaler.Marshal(e)
		if err != nil {
			return err
		}
	}

	return b.Publisher.PublishMessage(ctx, &Message{
		ID:              id,
		CausationID:     id,
		CorrelationID:   id,
		Source:          b.Config.Source + e.Source(),
		Type:            e.Topic(),
		SpecVersion:     CloudEventSpecVersion,
		DataContentType: contentType,
		DataSchema:      e.Schema(),
		Subject:         e.Subject(),
		Time:            time.Now().UTC(),
		Data:            data,
	})
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
	wg := &sync.WaitGroup{}
	for _, entries := range b.Registry.entries {
		go b.scheduler.ScheduleJobs(ctx, wg, entries)
	}
	wg.Wait()
	b.IsReady = true
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
