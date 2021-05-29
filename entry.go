package gluon

import "time"

type Entry struct {
	topic                string
	group                string
	retryTopic           string
	deadLetterQueueTopic string
	maxRetries           int
	retryBackoff         time.Duration
	subscriber           Subscriber
	subscriberFunc       SubscriberFunc
}

func (e *Entry) Group(g string) *Entry {
	e.group = g
	return e
}

func (e *Entry) RetryTopic(t string) *Entry {
	e.retryTopic = t
	return e
}

func (e *Entry) DeadLetterTopic(t string) *Entry {
	e.deadLetterQueueTopic = t
	return e
}

func (e *Entry) MaxRetries(d int) *Entry {
	e.maxRetries = d
	return e
}

func (e *Entry) RetryBackoff(d time.Duration) *Entry {
	e.retryBackoff = d
	return e
}

func (e *Entry) Subscriber(s Subscriber) *Entry {
	e.subscriber = s
	return e
}

func (e *Entry) SubscriberFunc(s SubscriberFunc) *Entry {
	e.subscriberFunc = s
	return e
}
