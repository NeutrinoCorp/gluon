package arch

import (
	"context"
	"sync"
)

// InternalMessageHandler is the message handler used by concrete drivers.
type InternalMessageHandler func(ctx context.Context, message *TransportMessage)

// Driver Is the transportation vendor (e.g. Apache Kafka, AWS SNS/SQS, Redis Streams) which implements
// `Gluon` internal mechanisms.
type Driver interface {
	SetParentBus(b *Bus)
	SetInternalHandler(h InternalMessageHandler)
	Start(context.Context) error
	Shutdown(context.Context) error
	Publish(ctx context.Context, topic string, message *TransportMessage) error
	Subscribe(ctx context.Context, topic string)
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
