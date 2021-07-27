package gcqrs

import (
	"context"
	"errors"

	"github.com/neutrinocorp/gluon"
)

var (
	// ErrCommandAlreadyExists requested command already exists within the command
	// bus registry
	ErrCommandAlreadyExists = errors.New("gcqrs: Command already exists")
)

// CommandHandler receives an specific command and executes derived logical tasks
type CommandHandler interface {
	// Handle execute operations of a command request
	Handle(context.Context, interface{}) error
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

func generateCommandTopic(b *gluon.Broker, cmd interface{}) string {
	topic := gluon.GenerateMessageKey("Command", true,
		cmd)
	return gluon.AddTopicPrefix(b, topic)
}

// Register attaches a command to a command handler
func (b *CommandBus) Register(cmd interface{}, h CommandHandler) error {
	topic := generateCommandTopic(b.broker, cmd)
	if b.broker.Registry.Exists(topic) {
		return ErrCommandAlreadyExists
	}
	b.broker.Topic(topic).Group(b.broker.Config.Group).SubscribeTo(cmd).
		SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
			return h.Handle(ctx, msg.Data)
		})
	return nil
}

// Dispatch makes a command request to the system
func (b *CommandBus) Dispatch(ctx context.Context, cmd interface{}) (string, error) {
	topic := generateCommandTopic(b.broker, cmd)
	return b.broker.Publish(ctx, topic, cmd)
}
