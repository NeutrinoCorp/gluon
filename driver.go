package gluon

import (
	"context"
	"sync"
)

// Driver is the vendor implementation of the message broker.
type Driver interface {
	WorkerFactory
	Publisher
	SetBroker(b *Broker)
	Close(context.Context) error
}

var (
	driversMu sync.RWMutex
	drivers   map[string]Driver = make(map[string]Driver)
)

// Register makes a message broker driver available for the Broker.
// If Register is called with a driver equals to nil,
// it panics.
func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("gluon: Given driver is nil")
	}
	drivers[name] = driver
}
