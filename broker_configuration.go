package gluon

import (
	"time"
)

// BrokerConfiguration Broker configuration options
type BrokerConfiguration struct {
	Organization string
	Service      string
	MajorVersion uint

	Group          string
	Source         string
	SchemaRegistry string
	IDFactory      IDFactory
	Marshaller     Marshaller
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
