package gkafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/gluon"
)

type consumerGroup struct {
	parentDriver *driver
	group        string

	consumerGroupInternal sarama.ConsumerGroup
}

func (s *consumerGroup) consume(ctx context.Context, sub *gluon.Subscriber) error {
	var err error
	s.consumerGroupInternal, err = sarama.NewConsumerGroup(s.parentDriver.parentBus.Addresses, s.group, s.parentDriver.config)
	if err != nil {
		return err
	}

	s.logErrorStream(s.consumerGroupInternal)
	go func() {
		for {
			_ = s.consumerGroupInternal.
				Consume(ctx, []string{sub.GetTopic()}, newInternalConsumerGroup(s.parentDriver, sub))
		}
	}()
	return nil
}

func (s *consumerGroup) close() error {
	return s.consumerGroupInternal.Close()
}

func (s *consumerGroup) logErrorStream(group sarama.ConsumerGroup) {
	if s.parentDriver.isLoggingEnabled() {
		go func() {
			for errConsumer := range group.Errors() {
				s.parentDriver.parentBus.Logger.Print(errConsumer)
			}
		}()
	}
}
