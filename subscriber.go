package gluon

import "context"

// SubscriberFunc is the actual handler (action) of a message represented as a function.
//
// Executes the given operation when a message has been received.
type SubscriberFunc func(context.Context, Message) error

// Subscriber is the actual handler (action) of a message represented as an struct
type Subscriber interface {
	// Handle executes the given operation when a message has been received
	Handle(context.Context, Message) error
}
