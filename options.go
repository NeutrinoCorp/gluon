package gluon

import (
	"context"
	"log"
)

type options struct {
	baseContext             context.Context
	remoteSchemaRegistryURL string
	majorVersion            int
	enableLogging           bool
	consumerGroup           string

	marshaler    Marshaler
	idFactory    IDFactory
	logger       *log.Logger
	driverConfig interface{}
	cluster      []string
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

type loggerOption struct {
	logger *log.Logger
}

func (o loggerOption) apply(opts *options) {
	opts.logger = o.logger
}

// WithLogger Set a global logger to output Bus internal operations.
func WithLogger(l *log.Logger) Option {
	return loggerOption{logger: l}
}

type remoteSchemaRegistryURLOption string

func (o remoteSchemaRegistryURLOption) apply(opts *options) {
	opts.remoteSchemaRegistryURL = string(o)
}

// WithRemoteSchemaRegistry Set a global remote schema registry reference (URL).
func WithRemoteSchemaRegistry(s string) Option {
	return remoteSchemaRegistryURLOption(s)
}

type majorVersionOption int

func (o majorVersionOption) apply(opts *options) {
	opts.majorVersion = int(o)
}

// WithMajorVersion Set a global major version for message schemas.
func WithMajorVersion(v int) Option {
	return majorVersionOption(v)
}

type enableLoggingOption bool

func (o enableLoggingOption) apply(opts *options) {
	opts.enableLogging = bool(o)
}

// WithLoggingOption Enable Bus internal operation(s) logs.
func WithLoggingOption(v bool) Option {
	return enableLoggingOption(v)
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
