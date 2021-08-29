package gkafka

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/gluon"
)

type driver struct {
	parentBus      *gluon.Bus
	messageHandler gluon.InternalMessageHandler
	config         *sarama.Config

	consumers []consumerStrategy
}

var (
	_               gluon.Driver = &driver{}
	defaultDriver   *driver
	driverSingleton = sync.Once{}
)

func init() {
	driverSingleton.Do(func() {
		defaultDriver = &driver{}
		gluon.Register("kafka", defaultDriver)
	})
}

func (d *driver) SetParentBus(b *gluon.Bus) {
	d.parentBus = b
	if cfg, ok := b.Configuration.Driver.(*sarama.Config); ok {
		d.config = cfg
	}
}

func (d *driver) SetInternalHandler(h gluon.InternalMessageHandler) {
	d.messageHandler = h
}

func (d *driver) Start(_ context.Context) error {
	return nil
}

func (d *driver) Shutdown(_ context.Context) error {
	errs := new(multierror.Error)
	for _, c := range d.consumers {
		if err := c.close(); err != nil {
			errs = multierror.Append(err, errs)
		}
	}

	return errs.ErrorOrNil()
}

func (d *driver) Publish(_ context.Context, message *gluon.TransportMessage) error {
	prod, err := sarama.NewSyncProducer(d.parentBus.Addresses, d.config)
	if err != nil {
		return err
	}

	_, _, err = prod.SendMessage(marshalKafkaMessage(message))
	if err != nil {
		return err
	}
	return prod.Close()
}

func (d *driver) Subscribe(ctx context.Context, subscriber *gluon.Subscriber) error {
	groupStr := d.parentBus.Configuration.ConsumerGroup
	if g := subscriber.GetGroup(); g != "" {
		groupStr = g // specified consumer group over global consumer group
	}
	consumer := newConsumerStrategy(d, groupStr)
	d.consumers = append(d.consumers, consumer)
	return consumer.consume(ctx, subscriber)
}

func (d *driver) isLoggingEnabled() bool {
	return d.parentBus.Configuration.EnableLogging && d.parentBus.Logger != nil
}
