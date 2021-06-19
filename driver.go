package gluon

import (
	"sync"
)

// Driver is the vendor implementation of the message broker.
type Driver interface {
	WorkerFactory
}

var (
	driversMu sync.RWMutex
	// DefaultDriver Gluon current driver implementation
	DefaultDriver Driver
)

// Register makes a message broker driver available for the Broker.
// If Register is called with a driver equals to nil,
// it panics.
func Register(driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("gluon: Given driver is nil")
	}
	DefaultDriver = driver
}
