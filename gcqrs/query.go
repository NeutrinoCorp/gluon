package gcqrs

import (
	"context"
	"errors"
	"sync"
)

// ErrQueryNotFound requested query was not found on query bus registry
var ErrQueryNotFound = errors.New("gcqrs: Query not found")

// Query unit which triggers a read operation within the given system.
//
// A query MUST have only one handler.
type Query interface {
	// Key query unique name
	Key() string
}

// QueryHandler receives an specific query and executes derived read tasks
type QueryHandler interface {
	// Query retrieves the query the given handler is attached to
	Query() Query
	// Handle execute read operations of a query request
	Handle(context.Context, Query) (interface{}, error)
}

// QueryBus infrastructure component which intends to route queries to their respective handlers.
//
// A query bus MUST be using in-memory messaging.
type QueryBus struct {
	mu       sync.Mutex
	registry map[string]QueryHandler
}

// NewQueryBus allocates a new query bus
func NewQueryBus() *QueryBus {
	return &QueryBus{
		mu:       sync.Mutex{},
		registry: make(map[string]QueryHandler),
	}
}

// Register attaches a query to a command handler
func (b *QueryBus) Register(q Query, h QueryHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registry[q.Key()] = h
}

// Dispatch makes a query request to the system
func (b *QueryBus) Ask(ctx context.Context, q Query) (interface{}, error) {
	h, ok := b.registry[q.Key()]
	if !ok {
		return nil, ErrQueryNotFound
	}
	return h.Handle(ctx, q)
}
