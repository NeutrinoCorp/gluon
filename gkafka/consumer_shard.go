package gkafka

import (
	"context"

	"github.com/hashicorp/go-multierror"

	"github.com/Shopify/sarama"

	"github.com/neutrinocorp/gluon"
)

type consumerShard struct {
	parentDriver *driver

	consumer  sarama.Consumer
	partition sarama.PartitionConsumer
}

var _ consumerStrategy = &consumerShard{}

func (c *consumerShard) consume(ctx context.Context, sub *gluon.Subscriber) error {
	var err error
	c.consumer, err = sarama.NewConsumer(c.parentDriver.parentBus.Addresses, c.parentDriver.config)
	if err != nil {
		return err
	}

	partitionID := c.getDefaultPartitionID(sub)
	c.partition, err = c.consumer.ConsumePartition(sub.GetTopic(), partitionID, c.getDefaultPartitionOffset())
	if err != nil {
		return err
	}

	go func() {
		for kMsg := range c.partition.Messages() {
			scopedCtx := context.TODO()
			msg := new(gluon.TransportMessage)
			unmarshalKafkaMessage(kMsg, msg)
			_ = c.parentDriver.messageHandler(scopedCtx, sub, msg)
		}
	}()
	return nil
}

func (c *consumerShard) getDefaultPartitionID(sub *gluon.Subscriber) int32 {
	if cfg, ok := sub.GetDriverConfiguration().(ConsumerConfiguration); ok {
		// If we try to cast the driver config without `ok` safety mechanism, program will panic.
		// Hence, this condition is required.
		return cfg.PartitionID
	}
	return 0
}

func (c *consumerShard) getDefaultPartitionOffset() int64 {
	if c.parentDriver.config != nil {
		return c.parentDriver.config.Consumer.Offsets.Initial
	}
	return sarama.OffsetNewest
}

func (c *consumerShard) logErrorStream(consumer sarama.PartitionConsumer) {
	if c.parentDriver.isLoggingEnabled() {
		go func() {
			for errConsumer := range consumer.Errors() {
				c.parentDriver.parentBus.Logger.Print(errConsumer)
			}
		}()
	}
}

func (c *consumerShard) logError(err error) {
	if c.parentDriver.isLoggingEnabled() {
		c.parentDriver.parentBus.Logger.Print(err)
	}
}

func (c *consumerShard) close() error {
	errs := new(multierror.Error)
	if err := c.consumer.Close(); err != nil {
		errs = multierror.Append(err, errs)
	}
	if err := c.partition.Close(); err != nil {
		errs = multierror.Append(err, errs)
	}
	return errs.ErrorOrNil()
}
