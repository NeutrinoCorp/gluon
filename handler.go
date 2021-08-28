package gluon

import "context"

// Handler Is a component used to subscribe to a topic.
type Handler interface {
	Handle(context.Context, *Message) error
}

// HandlerFunc Is an anonymous function used to subscribe to a topic.
type HandlerFunc func(context.Context, *Message) error
