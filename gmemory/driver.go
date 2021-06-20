package gmemory

import (
	"context"
	"sync"

	"github.com/neutrinocorp/gluon"
)

var (
	defaultBus       *bus
	defaultPublisher *MemoryPublisher
	busSingleton     = &sync.Once{}
)

func init() {
	busSingleton.Do(func() {
		defaultBus = newBus()
		defaultPublisher = &MemoryPublisher{
			bus: defaultBus,
		}
	})
	defaultBus.start()

	gluon.Register("memory", &Driver{
		bus:       defaultBus,
		publisher: defaultPublisher,
	})
}

type Driver struct {
	bus       *bus
	broker    *gluon.Broker
	publisher gluon.Publisher
}

var _ gluon.Driver = &Driver{}

func (d *Driver) NewWorker(b *gluon.Broker) gluon.Worker {
	return newWorker(b, d)
}

func (d Driver) PublishMessage(ctx context.Context, msg *gluon.Message) error {
	return d.publisher.PublishMessage(ctx, msg)
}

func (d *Driver) SetBroker(b *gluon.Broker) {
	d.broker = b
	d.bus.broker = b
}

func (d Driver) Close(ctx context.Context) error {
	d.bus.close()
	return nil
}
