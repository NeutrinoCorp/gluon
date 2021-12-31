package gluon

import (
	"context"

	"github.com/rs/zerolog"
)

type options struct {
	baseContext    context.Context
	schemaRegistry SchemaRegistry
	majorVersion   int
	enableLogging  bool
	consumerGroup  string

	marshaler           Marshaler
	idFactory           IDFactory
	logger              zerolog.Logger
	driverConfig        interface{}
	cluster             []string
	consumerMiddleware  []MiddlewareHandlerFunc
	publisherMiddleware []MiddlewarePublisherFunc
}

// Option set a specific configuration of a resource (e.g. bus).
type Option interface {
	apply(*options)
}

type baseContextOption struct {
	Ctx context.Context
}

func (o baseContextOption) apply(opts *options) {
	opts.baseContext = o.Ctx
}

// WithBaseContext Set the Bus base context.
func WithBaseContext(ctx context.Context) Option {
	return baseContextOption{
		Ctx: ctx,
	}
}

type marshalerOption struct {
	Marshaler Marshaler
}

func (o marshalerOption) apply(opts *options) {
	opts.marshaler = o.Marshaler
}

// WithMarshaler Set a marshaler strategy to encode/decode in-transit messages.
func WithMarshaler(m Marshaler) Option {
	return marshalerOption{
		Marshaler: m,
	}
}

type idFactoryOption struct {
	IDFactory IDFactory
}

func (o idFactoryOption) apply(opts *options) {
	opts.idFactory = o.IDFactory
}

// WithIDFactory Set a unique identifier factory for `Gluon` operations.
func WithIDFactory(f IDFactory) Option {
	return idFactoryOption{
		IDFactory: f,
	}
}

type schemaRegistryOption struct {
	SchemaRegistry SchemaRegistry
}

func (o schemaRegistryOption) apply(opts *options) {
	opts.schemaRegistry = o.SchemaRegistry
}

// WithSchemaRegistry Set a schema registry used by specific codecs (e.g. Apache Avro) to decode/encode in-transit messages.
func WithSchemaRegistry(s SchemaRegistry) Option {
	return schemaRegistryOption{
		SchemaRegistry: s,
	}
}

type loggerOption struct {
	logger zerolog.Logger
}

func (o loggerOption) apply(opts *options) {
	opts.logger = o.logger
}

// WithLogger Set a global logger to output Bus internal operations.
func WithLogger(l zerolog.Logger) Option {
	return loggerOption{logger: l}
}

type majorVersionOption int

func (o majorVersionOption) apply(opts *options) {
	opts.majorVersion = int(o)
}

// WithMajorVersion Set a global major version for message schemas.
func WithMajorVersion(v int) Option {
	return majorVersionOption(v)
}

type consumerGroupOption string

func (o consumerGroupOption) apply(opts *options) {
	opts.consumerGroup = string(o)
}

// WithConsumerGroup Set a global consumer group, useful for microservices.
func WithConsumerGroup(s string) Option {
	return consumerGroupOption(s)
}

type driverConfigOption struct {
	DriverConfig interface{}
}

func (o driverConfigOption) apply(opts *options) {
	opts.driverConfig = o.DriverConfig
}

// WithDriverConfiguration Set a configuration for a specific driver.
func WithDriverConfiguration(cfg interface{}) Option {
	return driverConfigOption{DriverConfig: cfg}
}

type clusterOption []string

func (o clusterOption) apply(opts *options) {
	opts.cluster = o
}

// WithCluster Set one up to N nodes for the Bus to use.
//
// Not applicable when using a local Bus.
func WithCluster(addr ...string) Option {
	return clusterOption(addr)
}

type consumerMiddlewareOption []MiddlewareHandlerFunc

func (o consumerMiddlewareOption) apply(opts *options) {
	opts.consumerMiddleware = o
}

// WithConsumerMiddleware Attach a chain of behaviour(s) for `Gluon` message consumption operations.
func WithConsumerMiddleware(f ...MiddlewareHandlerFunc) Option {
	return consumerMiddlewareOption(f)
}

type publisherMiddlewareOption []MiddlewarePublisherFunc

func (o publisherMiddlewareOption) apply(opts *options) {
	opts.publisherMiddleware = o
}

// WithPublisherMiddleware Attach a chain of behaviour(s) for `Gluon` message production of message operations.
func WithPublisherMiddleware(f ...MiddlewarePublisherFunc) Option {
	return publisherMiddlewareOption(f)
}
