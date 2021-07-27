package gcqrs

import (
	"context"
	"errors"
	"sync"

	"github.com/neutrinocorp/gluon"
)

var (
	// ErrQueryNotFound requested query was not found on query bus registry
	ErrQueryNotFound = errors.New("gcqrs: Query not found")
	// ErrQueryAlreadyExists requested query already exists within the query bus registry
	ErrQueryAlreadyExists = errors.New("gcqrs: Query already exists")
)

// QueryHandler receives an specific query and executes derived read tasks
type QueryHandler interface {
	// Handle execute read operations of a query request
	Handle(context.Context, interface{}) (interface{}, error)
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

func generateQueryTopic(q interface{}) string {
	return gluon.GenerateMessageKey("Query", true,
		q)
}

// Register attaches a query to a command handler
func (b *QueryBus) Register(q interface{}, h QueryHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	topic := generateQueryTopic(q)
	if _, ok := b.registry[topic]; ok {
		return ErrQueryAlreadyExists
	}
	b.registry[topic] = h
	return nil
}

// Ask makes a query request to the system
func (b *QueryBus) Ask(ctx context.Context, q interface{}) (interface{}, error) {
	h, ok := b.registry[generateQueryTopic(q)]
	if !ok {
		return nil, ErrQueryNotFound
	}
	return h.Handle(ctx, q)
}
