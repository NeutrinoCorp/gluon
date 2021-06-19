package gluon

import (
	"context"
	"sync"
)

// Worker is the unit of work of a subscription task.
//
// Use as driver abstraction to make vendor operations.
type Worker interface {
	Execute(context.Context, *sync.WaitGroup, *MessageHandler)
	Close(context.Context, *sync.WaitGroup, chan<- error)
}

// WorkerFactory is an artifact which is responsible for allocating new vendor-specific workers.
//
// It should come with the driver.
type WorkerFactory interface {
	// New allocates a new vendor-specific worker
	NewWorker(*Broker) Worker
}
