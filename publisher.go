package gluon

import "context"

type Publisher interface {
	Publish(context.Context, IntegrationEvent) error
}
