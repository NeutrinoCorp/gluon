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
	marshaller      Marshaller
	organization    string
	service         string
	majorVersion    uint
	group           string
	source          string
	schemaRegistry  string
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

type marshallerOption struct {
	Marshaller Marshaller
}

func (o marshallerOption) apply(opts *options) {
	opts.marshaller = o.Marshaller
}

// WithMarshaller sets the global marshaller to decode and encode messages
func WithMarshaller(m Marshaller) Option {
	return marshallerOption{
		Marshaller: m,
	}
}

type organizationOption string

func (o organizationOption) apply(opts *options) {
	opts.organization = string(o)
}

// WithOrganization sets the base organization name for internal broker ops
func WithOrganization(s string) Option {
	return organizationOption(s)
}

type serviceOption string

func (o serviceOption) apply(opts *options) {
	opts.service = string(o)
}

// WithService sets the base service name for internal broker ops
func WithService(s string) Option {
	return serviceOption(s)
}

type majorVersionOption uint

func (o majorVersionOption) apply(opts *options) {
	opts.majorVersion = uint(o)
}

// WithMajorVersion sets the base major version (from SemVer) for internal broker ops
func WithMajorVersion(s int) Option {
	if s > 0 {
		return majorVersionOption(s)
	}
	return majorVersionOption(1) // set default
}

type schemaOption string

func (o schemaOption) apply(opts *options) {
	opts.schemaRegistry = string(o)
}

// WithSchemaRegistry sets the base schema registry URL
func WithSchemaRegistry(s string) Option {
	return schemaOption(s)
}

type groupOption string

func (o groupOption) apply(opts *options) {
	opts.group = string(o)
}

// WithGroup sets the base consumer group
func WithGroup(g string) Option {
	return groupOption(g)
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

// WithMaxRetries sets the total amount of times to retry failed message consumptions
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

// WithMinRetryBackoff sets the minimum amount of time for backoff when retrying failed message consumptions
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

// WithMaxRetryBackoff sets the maximum amount of time for backoff when retrying failed message consumptions
func WithMaxRetryBackoff(d time.Duration) Option {
	if d <= 0 {
		d = defaultMaxRetries
	}

	return maxRetryBackoffOption(d)
}
