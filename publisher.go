package gluon

import "context"

// Publisher pushes messages into the message broker
type Publisher interface {
	// PublishMessage pushes given message into the message broker
	PublishMessage(context.Context, *Message) error
}
