package gcqrs

import (
	"context"
	"time"

	"github.com/neutrinocorp/gluon"
)

type Command interface {
	Key() string
}

type CommandHandler interface {
	Command() Command
	Handle(context.Context, Command) error
}

type CommandBus struct {
	broker *gluon.Broker
}

func NewCommandBus(b *gluon.Broker) *CommandBus {
	return &CommandBus{
		broker: b,
	}
}

func (c *CommandBus) Register(cmd Command, h CommandHandler) {
	c.broker.Topic(cmd.Key()).Group(c.broker.Config.Group).
		SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
			var cmdMsg Command
			if c.broker.Config.Marshaler != nil && c.broker.Config.Marshaler.ContentType() == msg.DataContentType {
				if err := c.broker.Config.Marshaler.Unmarshal(msg.Data, &cmdMsg); err != nil {
					return err
				}
			} else {
				cmdMsg = msg.Data.(Command) // default unmarshal
			}
			return h.Handle(ctx, cmdMsg)
		})
}

func (c *CommandBus) Exec(ctx context.Context, cmd Command) (string, error) {
	id, err := c.broker.Config.IDFactory.NewID()
	if err != nil {
		return "", err
	}

	var data interface{} = cmd
	contentType := ""
	if c.broker.Config.Marshaler != nil {
		contentType = c.broker.Config.Marshaler.ContentType()
		data, err = c.broker.Config.Marshaler.Marshal(cmd)
		if err != nil {
			return "", err
		}
	}

	return id, c.broker.Publisher.PublishMessage(ctx, &gluon.Message{
		ID:              id,
		CorrelationID:   id,
		CausationID:     id,
		Type:            cmd.Key(),
		Source:          c.broker.Config.Source,
		SpecVersion:     "1.0",
		DataContentType: contentType,
		Time:            time.Now().UTC(),
		Data:            data,
	})
}
