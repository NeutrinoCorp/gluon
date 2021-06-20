package gluon

import "time"

type BrokerConfiguration struct {
	Group      string
	Source     string
	Resiliency brokerResiliencyConfig
	IDFactory  IDFactory
	Marshaler  Marshaler
}

type brokerResiliencyConfig struct {
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration
}
