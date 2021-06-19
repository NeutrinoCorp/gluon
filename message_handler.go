package gluon

import "time"

// MessageHandler is the physical unit of the connection between a message and an action represented progammatically as a handler (a function or an struct).
//
// It also contains definitions of various resiliency mechanisms such as retry (using exponential + jitter backoff) and dead-letter queue.
//
// More information about retry mechanism can be found here: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
type MessageHandler struct {
	topic                string
	group                string
	retryTopic           string
	deadLetterQueueTopic string
	maxRetries           int
	minRetryBackoff      time.Duration
	maxRetryBackoff      time.Duration
	subscriber           Subscriber
	subscriberFunc       SubscriberFunc
}

// Group sets a consumer group (if driver allows) to the given handler.
//
// This sets up a mechanisms to group handlers as a unit of work. The message broker (e.g. Apache Kafka) will treat the group of handlers as
// one and will redistribute incoming messages within the group using balancing algorithms like round-robin.
//
// The use of this mechanism is highly recommended in microservice-based environments.
func (e *MessageHandler) Group(g string) *MessageHandler {
	e.group = g
	return e
}

// RetryTopic sets the retry topic for the given handler.
//
// It is recommended to have one retry topic per action/handler.
//
// More information about reliable reprocessing can be found here: https://eng.uber.com/reliable-reprocessing/
func (e *MessageHandler) RetryTopic(t string) *MessageHandler {
	e.retryTopic = t
	return e
}

// DeadLetterTopic sets the dead-letter queue (DLQ) topic for the given handler.
//
// It is recommended to have one DLQ topic per action/handler.
//
// More information about reliable reprocessing can be found here: https://eng.uber.com/reliable-reprocessing/
func (e *MessageHandler) DeadLetterTopic(t string) *MessageHandler {
	e.deadLetterQueueTopic = t
	return e
}

// MaxRetries sets the maximum number of retries for the given handler.
//
// The default number is 3 but it is recommended to set the number properly based on business requirements.
//
// More information about handling failures in Message-Driven applications can be found here: https://www.youtube.com/watch?v=SesEYHGhlLQ
func (e *MessageHandler) MaxRetries(d int) *MessageHandler {
	e.maxRetries = d
	return e
}

// MinRetryBackoff sets the minimum backoff duration for each retry of the given handler.
//
// The default duration is 1 second but it is recommended to set the number properly based on business requirements.
//
// More information about handling failures in Message-Driven applications can be found here: https://www.youtube.com/watch?v=SesEYHGhlLQ
func (e *MessageHandler) MinRetryBackoff(d time.Duration) *MessageHandler {
	e.minRetryBackoff = d
	return e
}

// MaxRetryBackoff sets the maximum backoff duration for each retry of the given handler.
//
// The default duration is 15 seconds but it is recommended to set the number properly based on business requirements.
//
// More information about handling failures in Message-Driven applications can be found here: https://www.youtube.com/watch?v=SesEYHGhlLQ
func (e *MessageHandler) MaxRetryBackoff(d time.Duration) *MessageHandler {
	e.maxRetryBackoff = d
	return e
}

// Subscriber sets the actual message handler.
//
// The given parameter is an struct which implements gluon.Subscriber interface.
func (e *MessageHandler) Subscriber(s Subscriber) *MessageHandler {
	e.subscriber = s
	return e
}

// SubscriberFunc sets the actual message handler.
//
// The given parameter is a function which complies with the gluon.SubscriberFunc type.
func (e *MessageHandler) SubscriberFunc(s SubscriberFunc) *MessageHandler {
	e.subscriberFunc = s
	return e
}

// -- Getters --

// GetTopic retrieves the current topic
func (e *MessageHandler) GetTopic() string {
	return e.topic
}

// GetGroup retrieves the current group
func (e *MessageHandler) GetGroup() string {
	return e.group
}

// GetRetryTopic retrieves the current retry topic
func (e *MessageHandler) GetRetryTopic() string {
	return e.retryTopic
}

// GetDeadLetterTopic retrieves the current dead-letter queue (DLQ) topic
func (e *MessageHandler) GetDeadLetterTopic() string {
	return e.deadLetterQueueTopic
}

// GetMaxRetries retrieves the current max retries number
func (e *MessageHandler) GetMaxRetries() int {
	return e.maxRetries
}

// GetMinRetryBackoff retrieves the current minimum retry backoff duration
func (e *MessageHandler) GetMinRetryBackoff() time.Duration {
	return e.minRetryBackoff
}

// GetMaxRetryBackoff retrieves the current maximum retry backoff duration
func (e *MessageHandler) GetMaxRetryBackoff() time.Duration {
	return e.maxRetryBackoff
}

// GetSubscriber retrieves the current subscriber
func (e *MessageHandler) GetSubscriber() Subscriber {
	return e.subscriber
}

// GetSubscriberFunc retrieves the current subscriber function
func (e *MessageHandler) GetSubscriberFunc() SubscriberFunc {
	return e.subscriberFunc
}
