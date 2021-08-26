package glocal

import (
	"context"
	"sync"

	"github.com/neutrinocorp/gluon/arch"
)

type driver struct {
	parentBus *arch.Bus
	registry  *partitionRegistry
	scheduler *scheduler
	handler   arch.InternalMessageHandler
}

var (
	_               arch.Driver = &driver{}
	defaultDriver   *driver
	driverSingleton = sync.Once{}
)

func init() {
	driverSingleton.Do(func() {
		defaultDriver = &driver{
			registry:  newPartitionRegistry(),
			scheduler: newScheduler(),
		}
	})
	arch.Register("local", defaultDriver)
}

func (d *driver) Shutdown(_ context.Context) error {
	d.scheduler.close()
	return nil
}

func (d *driver) SetParentBus(b *arch.Bus) {
	d.parentBus = b
}

func (d *driver) SetInternalHandler(h arch.InternalMessageHandler) {
	d.handler = h
}

func (d *driver) Publish(_ context.Context, topic string, message *arch.TransportMessage) error {
	topicPartition := d.registry.get(topic)
	if topicPartition == nil {
		topicPartition = newPartition()
		d.registry.set(topic, topicPartition)
	}
	topicPartition.push(message)
	d.scheduler.notify(topic)
	return nil
}

func (d *driver) Subscribe(_ context.Context, _ string) {
}

func (d *driver) Start(_ context.Context) error {
	go func() {
		// TODO: Fix NAck and Ack mechanisms
		for topic := range d.scheduler.notificationStream {
			go func(t string) {
				p := defaultDriver.registry.get(t)
				if p == nil {
					return
				}

				msg := p.lastMessage
				d.handler(context.Background(), &msg)
			}(topic)
		}
	}()
	return nil
}
