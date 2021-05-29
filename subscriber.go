package gluon

import "context"

type SubscriberFunc func(context.Context, IntegrationEvent) error

type Subscriber interface {
	Handle(context.Context, IntegrationEvent) error
}
