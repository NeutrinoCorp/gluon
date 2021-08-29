package gluon

// Subscriber Is an entry for the subscriber registry component.
//
// It contains metadata for a specific consumer.
type Subscriber struct {
	key string

	group        string
	handler      Handler
	handlerFunc  HandlerFunc
	driverConfig interface{}
}

func newSubscriber(key string) *Subscriber {
	return &Subscriber{
		key: key,
	}
}

// Group Set a consumer group (if Driver allows them).
//
// A consumer group is a mechanism used to consume messages in parallel by multiple workers as a single unit.
//
// This kind of mechanisms are very useful when consuming messages from within a microservice environment, where
// each node from the microservice cluster gets a message every specific time (depending on the load balancing algorithm).
func (e *Subscriber) Group(g string) *Subscriber {
	e.group = g
	return e
}

// Handler Set a Handler component.
//
// In-transit messages will go straight through to Handler.Handle function.
func (e *Subscriber) Handler(h Handler) *Subscriber {
	e.handler = h
	return e
}

// HandlerFunc Set a HandlerFunc component.
//
// In-transit messages will go straight through to the function.
func (e *Subscriber) HandlerFunc(h HandlerFunc) *Subscriber {
	e.handlerFunc = h
	return e
}

// DriverConfiguration Set configuration for a specific driver.
func (e *Subscriber) DriverConfiguration(cfg interface{}) *Subscriber {
	e.driverConfig = cfg
	return e
}

// GetTopic Retrieve the Subscriber's topic name.
func (e Subscriber) GetTopic() string {
	return e.key
}

// GetDefaultHandler Retrieve the Subscriber's default HandlerFunc.
//
// It will return the defined HandlerFunc by default. If no HandlerFunc was specified, then the Handler.Handle function
// is returned.
func (e Subscriber) GetDefaultHandler() HandlerFunc {
	if e.handlerFunc != nil {
		return e.handlerFunc
	}
	return e.handler.Handle
}

// GetHandler Retrieve the Subscriber's Handler.
func (e Subscriber) GetHandler() Handler {
	return e.handler
}

// GetHandlerFunc Retrieve the Subscriber's HandlerFunc.
func (e Subscriber) GetHandlerFunc() HandlerFunc {
	return e.handlerFunc
}

// GetGroup Retrieve the Subscriber's consumer group.
func (e Subscriber) GetGroup() string {
	return e.group
}

// GetDriverConfiguration Get the configuration of a specific driver.
func (e *Subscriber) GetDriverConfiguration() interface{} {
	return e.driverConfig
}
