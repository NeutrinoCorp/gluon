package gluon

import (
	"context"
	"time"
)

const (
	defaultMaxRetries      = 3
	defaultMinRetryBackoff = time.Millisecond * 500
	defaultMaxRetryBackoff = time.Second * 15
)

type options struct {
	baseContext     context.Context
	publisher       Publisher
	idFactory       IDFactory
	marshaler       Marshaler
	source          string
	hosts           []string
	maxRetries      int
	minRetryBackoff time.Duration
	maxRetryBackoff time.Duration
}

// Option set an specific configuration of a resource (e.g. broker)
type Option interface {
	apply(*options)
}

type baseContextOption struct {
	Ctx context.Context
}

func (o baseContextOption) apply(opts *options) {
	opts.baseContext = o.Ctx
}

// WithBaseContext sets the base context of the broker
func WithBaseContext(ctx context.Context) Option {
	return baseContextOption{
		Ctx: ctx,
	}
}

type publisherOption struct {
	Publisher Publisher
}

func (o publisherOption) apply(opts *options) {
	opts.publisher = o.Publisher
}

// WithPublisher sets the global publisher to write messages to stream
func WithPublisher(p Publisher) Option {
	return publisherOption{
		Publisher: p,
	}
}

type idFactoryOption struct {
	IDFactory IDFactory
}

func (o idFactoryOption) apply(opts *options) {
	opts.idFactory = o.IDFactory
}

// WithIDFactory sets the global unique identifier factory for message generation
func WithIDFactory(f IDFactory) Option {
	return idFactoryOption{
		IDFactory: f,
	}
}

type marshalerOption struct {
	Marshaler Marshaler
}

func (o marshalerOption) apply(opts *options) {
	opts.marshaler = o.Marshaler
}

// WithMarshaler sets the global marshaler to decode and encode messages
func WithMarshaler(m Marshaler) Option {
	return marshalerOption{
		Marshaler: m,
	}
}

type sourceOption string

func (o sourceOption) apply(opts *options) {
	opts.source = string(o)
}

// WithSource sets the main source when generating messages (used by CloudEvents)
func WithSource(s string) Option {
	return sourceOption(s)
}

type hostsOption []string

func (o hostsOption) apply(opts *options) {
	opts.hosts = o
}

// WithHosts sets the addresses names and ports for external infrastructures
func WithHosts(hosts ...string) Option {
	return hostsOption(hosts)
}

type maxRetriesOption int

func (o maxRetriesOption) apply(opts *options) {
	opts.maxRetries = int(o)
}

// WithMaxRetries sets the total ammount of times to retry failed message consumptions
func WithMaxRetries(d int) Option {
	if d < 0 {
		d = defaultMaxRetries
	}
	return maxRetriesOption(d)
}

type minRetryBackoffOption time.Duration

func (o minRetryBackoffOption) apply(opts *options) {
	opts.minRetryBackoff = time.Duration(o)
}

// WithMinRetryBackoff sets the minimum ammount of time for backoff when retrying failed message consumptions
func WithMinRetryBackoff(d time.Duration) Option {
	if d <= 0 {
		d = defaultMinRetryBackoff
	}

	return minRetryBackoffOption(d)
}

type maxRetryBackoffOption time.Duration

func (o maxRetryBackoffOption) apply(opts *options) {
	opts.maxRetryBackoff = time.Duration(o)
}

// WithMaxRetryBackoff sets the maximum ammount of time for backoff when retrying failed message consumptions
func WithMaxRetryBackoff(d time.Duration) Option {
	if d <= 0 {
		d = defaultMaxRetries
	}

	return maxRetryBackoffOption(d)
}
