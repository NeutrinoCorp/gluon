package gkafka

import (
	"context"

	"github.com/neutrinocorp/gluon"
)

type consumerStrategy interface {
	consume(ctx context.Context, sub *gluon.Subscriber) error
	close() error
}

func newConsumerStrategy(d *driver, group string) consumerStrategy {
	if group != "" {
		return &consumerGroup{
			parentDriver: d,
			group:        group,
		}
	}
	return &consumerShard{
		parentDriver: d,
	}
}
