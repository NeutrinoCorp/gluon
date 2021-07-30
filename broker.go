package gluon

import (
	"context"
	"errors"
	"os"
	"strings"
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
			Organization:   options.organization,
			Service:        options.service,
			MajorVersion:   options.majorVersion,
			Group:          options.group,
			Source:         options.source,
			SchemaRegistry: options.schemaRegistry,
			IDFactory:      options.idFactory,
			Marshaller:     options.marshaller,
			Networking: brokerNetworkConfig{
				Hosts: options.hosts,
			},
			Resiliency: brokerResiliencyConfig{
				MaxRetries:      options.maxRetries,
				MinRetryBackoff: options.minRetryBackoff,
				MaxRetryBackoff: options.maxRetryBackoff,
			},
		},
	}
	drivers[driver].SetBroker(b)
	return b
}

// sets required default values for broker's ops
func newBrokerDefaultOptions(driver string) options {
	hostname, _ := os.Hostname()
	hostname = strings.ReplaceAll(hostname, ".", "-")
	return options{
		baseContext:     context.Background(),
		idFactory:       RandomIDFactory{},
		publisher:       drivers[driver],
		hosts:           make([]string, 0),
		maxRetries:      defaultMaxRetries,
		minRetryBackoff: defaultMinRetryBackoff,
		maxRetryBackoff: defaultMaxRetryBackoff,
		group:           hostname,
		organization:    "acme",
		service:         hostname,
		majorVersion:    1,
		source:          hostname,
	}
}

// Topic sets a new message consumer using the given parameter as key (aka. topic)
//
// DO NOT forget to use SubscribedTo() method if using a marshaller
func (b *Broker) Topic(t string) *Consumer {
	return b.Registry.Topic(t)
}

// Message sets a new message consumer using properties of the given message
func (b *Broker) Message(m Message) *Consumer {
	return b.Registry.Message(m)
}

// Event sets a new event consumer using the given event topic
func (b *Broker) Event(e interface{}) *Consumer {
	return b.Registry.Topic(GenerateEventTopic(b, e)).SubscribeTo(e)
}

// Publish propagates the given event to the whole system through the message broker
func (b *Broker) Publish(ctx context.Context, topic string, e interface{}) (string, error) {
	id, err := b.Config.IDFactory.NewID()
	if err != nil {
		return "", err
	}
	return id, b.publish(ctx, topic, id, id, id, e)
}

type PublishFromParentArgs struct {
	Topic         string
	CausationID   string
	CorrelationID string
	Event         interface{}
}

// PublishFromParent propagates the given event to the whole system through the message broker.
//
// Attach a parent to the given event as it improves observability.
//
// More information may be found here: https://blog.arkency.com/correlation-id-and-causation-id-in-evented-systems/
func (b *Broker) PublishFromParent(ctx context.Context, args PublishFromParentArgs) (string, error) {
	id, err := b.Config.IDFactory.NewID()
	if err != nil {
		return "", err
	}
	return id, b.publish(ctx, args.Topic, id, args.CausationID,
		args.CorrelationID, args.Event)
}

type PublishRawArgs struct {
	Topic         string
	MessageID     string
	CausationID   string
	CorrelationID string
	Event         interface{}
}

// PublishRaw propagates the given event using all the arguments passed
func (b *Broker) PublishRaw(ctx context.Context, args PublishRawArgs) error {
	return b.publish(ctx, args.Topic, args.MessageID, args.CausationID,
		args.CorrelationID, args.Event)
}

// Publish propagates the given event to the whole system through the message broker
func (b *Broker) publish(ctx context.Context, topic, id, causationID, correlationID string,
	ev interface{}) error {
	var err error
	var data = ev
	contentType := ""
	if b.Config.Marshaller != nil {
		contentType = b.Config.Marshaller.ContentType()
		data, err = b.Config.Marshaller.Marshal(ev)
		if err != nil {
			return err
		}
	}

	return b.Publisher.PublishMessage(ctx, &Message{
		ID:              id,
		CausationID:     causationID,
		CorrelationID:   correlationID,
		Source:          b.Config.Source,
		Type:            topic,
		SpecVersion:     CloudEventSpecVersion,
		DataContentType: contentType,
		DataSchema:      b.Config.SchemaRegistry,
		Subject:         b.Config.Source + "/" + b.Config.Service,
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
		_ = b.Shutdown(b.BaseContext)
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
