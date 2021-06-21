package gluon

import "context"

// Subscriber is the actual handler (action) of a message represented as an struct
type Subscriber interface {
	// Handle executes the given operation when a message has been received
	Handle(context.Context, Message) error
}

// SubscriberFunc is the actual handler (action) of a message represented as a function.
//
// Executes the given operation when a message has been received.
type SubscriberFunc func(context.Context, Message) error

// Handle executes the given operation when a message has been received
func (f SubscriberFunc) Handle(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}
