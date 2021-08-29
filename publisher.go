package gluon

import "context"

// Publisher Is a component used by `Gluon` to propagate low-level messages.
type Publisher interface {
	// Publish Propagate a low-level message (CloudEvent) to the message bus.
	Publish(ctx context.Context, message *TransportMessage) error
}

// PublisherFunc Is an anonymous function used by `Gluon` to propagate low-level messages.
type PublisherFunc func(ctx context.Context, message *TransportMessage) error

// MiddlewarePublisherFunc Is an anonymous function used to add behaviour to a publishing process.
//
// This pattern is also known as Chain of Responsibility (CoR).
type MiddlewarePublisherFunc func(next PublisherFunc) PublisherFunc
