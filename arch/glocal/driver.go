package glocal

import (
	"context"
	"sync"

	"github.com/neutrinocorp/gluon/arch"
)

type driver struct {
	mu              sync.Mutex
	parentBus       *arch.Bus
	topicPartitions map[string]*partition // Key: partition_index#topic_name
	schedulerBuffer *schedulerBuffer
	handler         arch.InternalMessageHandler
}

var (
	_               arch.Driver = &driver{}
	defaultDriver   *driver
	driverSingleton = sync.Once{}
)

func init() {
	driverSingleton.Do(func() {
		defaultDriver = &driver{
			mu:              sync.Mutex{},
			topicPartitions: map[string]*partition{},
			schedulerBuffer: newSchedulerBuffer(),
		}
	})
	arch.Register("local", defaultDriver)
}

func (d *driver) Shutdown(_ context.Context) error {
	d.schedulerBuffer.close()
	return nil
}

func (d *driver) SetParentBus(b *arch.Bus) {
	d.parentBus = b
}

func (d *driver) SetInternalHandler(h arch.InternalMessageHandler) {
	d.handler = h
}

func (d *driver) Publish(_ context.Context, topic string, message *arch.TransportMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	topicPartition := d.topicPartitions[topic]
	if topicPartition == nil {
		topicPartition = newPartition()
		d.topicPartitions[topic] = topicPartition
	}
	topicPartition.push(message)
	d.schedulerBuffer.notify(topic)
	return nil
}

func (d *driver) Subscribe(_ context.Context, _ string) {}

func (d *driver) Start(_ context.Context) error {
	go d.startSubscriberTaskScheduler()
	return nil
}

func (d *driver) startSubscriberTaskScheduler() {
	for topic := range d.schedulerBuffer.notificationStream {
		go func(t string) {
			if p := defaultDriver.topicPartitions[t]; p != nil {
				_ = d.handler(context.Background(), &p.lastMessage)
			}
		}(topic)
	}
}
