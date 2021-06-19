package gmemory

import (
	"context"
	"sync"

	"github.com/neutrinocorp/gluon"
)

var (
	defaultBus   *bus
	busSingleton = &sync.Once{}
)

func init() {
	busSingleton.Do(func() {
		defaultBus = newBus()
	})
	defaultBus.start()

	gluon.Register(&Driver{
		bus: defaultBus,
	})
}

type Driver struct {
	bus    *bus
	broker *gluon.Broker
}

var _ gluon.Driver = &Driver{}

func (d *Driver) NewWorker(b *gluon.Broker) gluon.Worker {
	return newWorker(b, d)
}

func (d Driver) PublishMessage(ctx context.Context, msg *gluon.Message) error {
	go d.bus.publish(msg)
	return nil
}

func (d *Driver) SetBroker(b *gluon.Broker) {
	d.broker = b
	d.bus.broker = b
}

func (d Driver) Close(ctx context.Context) error {
	d.bus.close()
	return nil
}
