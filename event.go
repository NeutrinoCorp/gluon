package gluon

// Event is a high-level unit of communication used to transmit data between streams
type Event interface {
	Topic() string
	Schema() string
	Subject() string
	Source() string
}
