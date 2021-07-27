package gluon

import "time"

// Consumer is the physical unit of the connection between a message and an action represented programmatically as a handler (a function or an struct).
//
// It also contains definitions of various resiliency mechanisms such as retry (using exponential + jitter backoff) and dead-letter queue.
//
// More information about retry mechanism can be found here: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
type Consumer struct {
	topic                string
	group                string
	retryTopic           string
	deadLetterQueueTopic string
	maxRetries           int
	minRetryBackoff      time.Duration
	maxRetryBackoff      time.Duration
	subscriber           Subscriber
	subscriberFunc       SubscriberFunc
	message              interface{}
}

// Group sets a consumer group (if driver allows) to the given handler.
//
// This sets up a mechanisms to group handlers as a unit of work. The message broker (c.g. Apache Kafka) will treat the group of handlers as
// one and will redistribute incoming messages within the group using balancing algorithms like round-robin.
//
// The use of this mechanism is highly recommended in microservice-based environments.
func (c *Consumer) Group(g string) *Consumer {
	c.group = g
	return c
}

// RetryTopic sets the retry topic for the given handler.
//
// It is recommended to have one retry topic per action/handler.
//
// More information about reliable reprocessing can be found here: https://eng.uber.com/reliable-reprocessing/
func (c *Consumer) RetryTopic(t string) *Consumer {
	c.retryTopic = t
	return c
}

// DeadLetterTopic sets the dead-letter queue (DLQ) topic for the given handler.
//
// It is recommended to have one DLQ topic per action/handler.
//
// More information about reliable reprocessing can be found here: https://eng.uber.com/reliable-reprocessing/
func (c *Consumer) DeadLetterTopic(t string) *Consumer {
	c.deadLetterQueueTopic = t
	return c
}

// MaxRetries sets the maximum number of retries for the given handler.
//
// The default number is 3 but it is recommended to set the number properly based on business requirements.
//
// More information about handling failures in Message-Driven applications can be found here: https://www.youtube.com/watch?v=SesEYHGhlLQ
func (c *Consumer) MaxRetries(d int) *Consumer {
	c.maxRetries = d
	return c
}

// MinRetryBackoff sets the minimum backoff duration for each retry of the given handler.
//
// The default duration is 500 milliseconds but it is recommended to set the number properly based on business requirements.
//
// More information about handling failures in Message-Driven applications can be found here: https://www.youtube.com/watch?v=SesEYHGhlLQ
func (c *Consumer) MinRetryBackoff(d time.Duration) *Consumer {
	c.minRetryBackoff = d
	return c
}

// MaxRetryBackoff sets the maximum backoff duration for each retry of the given handler.
//
// The default duration is 15 seconds but it is recommended to set the number properly based on business requirements.
//
// More information about handling failures in Message-Driven applications can be found here: https://www.youtube.com/watch?v=SesEYHGhlLQ
func (c *Consumer) MaxRetryBackoff(d time.Duration) *Consumer {
	c.maxRetryBackoff = d
	return c
}

// Subscriber sets the actual message handler.
//
// The given parameter is an struct which implements gluon.Subscriber interface.
func (c *Consumer) Subscriber(s Subscriber) *Consumer {
	c.subscriber = s
	return c
}

// SubscriberFunc sets the actual message handler.
//
// The given parameter is a function which complies with the gluon.SubscriberFunc type.
func (c *Consumer) SubscriberFunc(s SubscriberFunc) *Consumer {
	c.subscriberFunc = s
	return c
}

// SubscribeTo sets the subscription to the given message.
func (c *Consumer) SubscribeTo(msg interface{}) *Consumer {
	c.message = msg
	return c
}

// -- Getters --

// GetTopic retrieves the current topic
func (c *Consumer) GetTopic() string {
	return c.topic
}

// GetGroup retrieves the current group
func (c *Consumer) GetGroup() string {
	return c.group
}

// GetRetryTopic retrieves the current retry topic
func (c *Consumer) GetRetryTopic() string {
	return c.retryTopic
}

// GetDeadLetterTopic retrieves the current dead-letter queue (DLQ) topic
func (c *Consumer) GetDeadLetterTopic() string {
	return c.deadLetterQueueTopic
}

// GetMaxRetries retrieves the current max retries number
func (c *Consumer) GetMaxRetries() int {
	return c.maxRetries
}

// GetMinRetryBackoff retrieves the current minimum retry backoff duration
func (c *Consumer) GetMinRetryBackoff() time.Duration {
	return c.minRetryBackoff
}

// GetMaxRetryBackoff retrieves the current maximum retry backoff duration
func (c *Consumer) GetMaxRetryBackoff() time.Duration {
	return c.maxRetryBackoff
}

// GetSubscriber retrieves the current subscriber
func (c *Consumer) GetSubscriber() Subscriber {
	return c.subscriber
}

// GetSubscriberFunc retrieves the current subscriber function
func (c *Consumer) GetSubscriberFunc() SubscriberFunc {
	return c.subscriberFunc
}

// GetSubscribedMessage retrieves the current subscription message
func (c *Consumer) GetSubscribedMessage() interface{} {
	return c.message
}
