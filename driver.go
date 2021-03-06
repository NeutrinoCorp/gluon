package gluon

import (
	"context"
	"sync"
)

// Driver Is the transportation vendor (e.g. Apache Kafka, AWS SNS/SQS, Redis Streams) which implements
// `Gluon` internal mechanisms.
type Driver interface {
	SetParentBus(b *Bus)
	SetInternalHandler(h InternalMessageHandler)
	Start(context.Context) error
	Shutdown(context.Context) error
	Subscribe(ctx context.Context, subscriber *Subscriber) error
	// Publish Propagate a low-level message (CloudEvent) to the message bus.
	Publish(ctx context.Context, message *TransportMessage) error
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

// Register makes a message broker driver available for the Bus.
//
// If Register is called with a driver equals to nil, it panics.
func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("gluon: Given driver is nil")
	}
	drivers[name] = driver
}
