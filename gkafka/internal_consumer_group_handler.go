package gkafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/gluon"
)

type internalConsumerGroupHandler struct {
	parentDriver *driver
	sub          *gluon.Subscriber
}

var _ sarama.ConsumerGroupHandler = &internalConsumerGroupHandler{}

func newInternalConsumerGroup(d *driver, s *gluon.Subscriber) *internalConsumerGroupHandler {
	return &internalConsumerGroupHandler{
		parentDriver: d,
		sub:          s,
	}
}

func (i *internalConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (i *internalConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (i *internalConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	for kMsg := range claim.Messages() {
		scopedCtx := context.TODO()
		msg := new(gluon.TransportMessage)
		unmarshalKafkaMessage(kMsg, msg)
		err := i.parentDriver.messageHandler(scopedCtx, i.sub, msg)
		if err == nil {
			session.MarkMessage(kMsg, "")
		}
	}
	return nil
}

func (i *internalConsumerGroupHandler) logError(err error) {
	if i.parentDriver.isLoggingEnabled() {
		i.parentDriver.parentBus.Logger.Print(err)
	}
}
