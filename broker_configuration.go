package gluon

import "time"

type BrokerConfiguration struct {
	MaxRetries   int
	RetryBackoff time.Duration
}
