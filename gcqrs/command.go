package gcqrs

import (
	"context"
	"time"

	"github.com/neutrinocorp/gluon"
)

// Command unit which triggers an operation -or entity state mutation- within the given system.
//
// A command MUST have only one handler.
type Command interface {
	// Key command unique name
	Key() string
}

// CommandHandler receives an specific command and executes derived logical tasks
type CommandHandler interface {
	// Command retrieves the command the given handler is attached to
	Command() Command
	// Handle execute operations of a command request
	Handle(context.Context, Command) error
}

// CommandBus infrastructure component which intends to route commands to their respective handlers
type CommandBus struct {
	broker *gluon.Broker
}

// NewCommandBus allocates a new command bus
func NewCommandBus(b *gluon.Broker) *CommandBus {
	return &CommandBus{
		broker: b,
	}
}

// Register attaches a command to a command handler
func (b *CommandBus) Register(cmd Command, h CommandHandler) {
	b.broker.Topic(cmd.Key()).Group(b.broker.Config.Group).
		SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
			var cmdMsg Command
			if b.broker.Config.Marshaler != nil && b.broker.Config.Marshaler.ContentType() == msg.DataContentType {
				if err := b.broker.Config.Marshaler.Unmarshal(msg.Data, &cmdMsg); err != nil {
					return err
				}
			} else {
				cmdMsg = msg.Data.(Command) // default unmarshal
			}
			return h.Handle(ctx, cmdMsg)
		})
}

// Dispatch makes a command request to the system
func (b *CommandBus) Dispatch(ctx context.Context, cmd Command) (string, error) {
	id, err := b.broker.Config.IDFactory.NewID()
	if err != nil {
		return "", err
	}

	var data interface{} = cmd
	contentType := ""
	if b.broker.Config.Marshaler != nil {
		contentType = b.broker.Config.Marshaler.ContentType()
		data, err = b.broker.Config.Marshaler.Marshal(cmd)
		if err != nil {
			return "", err
		}
	}

	return id, b.broker.Publisher.PublishMessage(ctx, &gluon.Message{
		ID:              id,
		CorrelationID:   id,
		CausationID:     id,
		Type:            cmd.Key(),
		Source:          b.broker.Config.Source,
		SpecVersion:     gluon.CloudEventSpecVersion,
		DataContentType: contentType,
		Time:            time.Now().UTC(),
		Data:            data,
	})
}
