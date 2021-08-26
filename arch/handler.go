package arch

import "context"

// Handler Is one of the basic components used to subscribe to a topic.
type Handler interface {
	Handle(context.Context, *Message) error
}

type HandlerFunc func(context.Context, *Message) error
