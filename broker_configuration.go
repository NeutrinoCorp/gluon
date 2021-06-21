package gluon

import (
	"time"
)

// BrokerConfiguration Broker configuration options
type BrokerConfiguration struct {
	Group          string
	Source         string
	SchemaRegistry string
	IDFactory      IDFactory
	Marshaler      Marshaler
	Networking     brokerNetworkConfig
	Resiliency     brokerResiliencyConfig
}

type brokerResiliencyConfig struct {
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration
}

type brokerNetworkConfig struct {
	Hosts []string
}
