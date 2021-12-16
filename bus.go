package gluon

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/hashicorp/go-multierror"
)

// ErrBusClosed Cannot perform the action with a closed Bus.
var ErrBusClosed = errors.New("gluon: The bus is closed")

// Bus Is a facade component used to interact with foreign systems through streaming messaging mechanisms.
type Bus struct {
	BaseContext         context.Context
	Marshaler           Marshaler
	Factories           Factories
	Configuration       BusConfiguration
	Logger              *log.Logger
	Addresses           []string
	consumerMiddleware  []MiddlewareHandlerFunc
	publisherMiddleware []MiddlewarePublisherFunc

	driver             Driver
	schemaRegistry     *schemaRegistry
	subscriberRegistry *subscriberRegistry
}

// NewBus Allocate a new Bus with default configurations.
func NewBus(driver string, opts ...Option) *Bus {
	options := newBusDefaults()
	for _, o := range opts {
		o.apply(&options)
	}
	return &Bus{
		BaseContext: options.baseContext,
		Marshaler:   options.marshaler,
		Factories: Factories{
			IDFactory: options.idFactory,
		},
		Configuration: BusConfiguration{
			RemoteSchemaRegistryURI: options.remoteSchemaRegistryURL,
			MajorVersion:            options.majorVersion,
			Driver:                  options.driverConfig,
			ConsumerGroup:           options.consumerGroup,
		},
		Logger:              options.logger,
		Addresses:           options.cluster,
		consumerMiddleware:  options.consumerMiddleware,
		publisherMiddleware: options.publisherMiddleware,
		driver:              drivers[driver],
		schemaRegistry:      newSchemaRegistry(),
		subscriberRegistry:  newSubscriberRegistry(),
	}
}

func newBusDefaults() options {
	return options{
		baseContext:             context.Background(),
		remoteSchemaRegistryURL: "",
		majorVersion:            1,
		enableLogging:           false,
		consumerGroup:           "",
		marshaler:               defaultMarshaler,
		idFactory:               defaultIDFactory,
		logger:                  nil,
		driverConfig:            nil,
		cluster:                 nil,
	}
}

// RegisterSchema Link a message schema to specific metadata (MessageMetadata) and store it for Bus further operations.
func (b *Bus) RegisterSchema(schema interface{}, opts ...SchemaRegistryOption) {
	options := schemaRegistryOptions{}
	for _, o := range opts {
		o.apply(&options)
	}
	b.schemaRegistry.register(schema, MessageMetadata{
		Topic:         options.topic,
		Source:        options.source,
		SchemaURI:     options.schemaURI,
		SchemaVersion: options.version,
	})
}

// ListenAndServe Bootstrap and start a Bus along its internal components (subscribers).
func (b *Bus) ListenAndServe() error {
	b.driver.SetParentBus(b)
	b.Marshaler.SetParentBus(b)
	b.driver.SetInternalHandler(getInternalHandler(b))
	if err := b.driver.Start(b.BaseContext); err != nil {
		return err
	}
	return b.startSubscriberJobs()
}

func (b *Bus) startSubscriberJobs() error {
	errs := new(multierror.Error)
	for _, subs := range b.subscriberRegistry.registry {
		for _, s := range subs {
			err := b.driver.Subscribe(b.BaseContext, s)
			if err != nil {
				errs = multierror.Append(err, errs)
			}
		}
	}

	return errs.ErrorOrNil()
}

// Subscribe Set a subscription task using schema metadata.
//
// It will return nil if no schema was found on local schema registry.
func (b *Bus) Subscribe(schema interface{}) *Subscriber {
	meta, err := b.schemaRegistry.get(schema)
	if err != nil {
		return nil
	}
	entry := newSubscriber(meta.Topic)
	b.subscriberRegistry.register(meta.Topic, entry)
	return entry
}

// SubscribeTopic Set a subscription task using a raw topic name.
func (b *Bus) SubscribeTopic(topic string) *Subscriber {
	entry := newSubscriber(topic)
	b.subscriberRegistry.register(topic, entry)
	return entry
}

// ListSubscribersFromTopic Get the subscription task queue of a registered topic.
func (b *Bus) ListSubscribersFromTopic(t string) []*Subscriber {
	return b.subscriberRegistry.get(t)
}

// Publish Propagate a message to the ecosystem using the internal topic registry agent to generate the topic.
//
// 	Note: To propagate correlation and causation IDs, use Subscription's context.
func (b *Bus) Publish(ctx context.Context, data interface{}) error {
	meta, err := b.schemaRegistry.get(data)
	if err != nil {
		return err
	}
	msg, err := b.generateTransportMessage(meta, data)
	if err != nil {
		return err
	}
	return b.publish(ctx, msg)
}

// PublishWithTopic Propagate a message to the ecosystem using the internal topic registry agent to generate the topic.
//
// 	Note: To propagate correlation and causation IDs, use Subscription's context.
func (b *Bus) PublishWithTopic(ctx context.Context, topic string, data interface{}) error {
	meta := b.schemaRegistry.getByTopic(topic)
	msg, err := b.generateTransportMessage(meta, data)
	if err != nil {
		return err
	}
	return b.publish(ctx, msg)
}

// PublishWithSubject Propagate a message to the ecosystem using the internal topic registry agent to generate the topic.
//
// This method also exposes the `Subject` property to define the CloudEvent property with the same name.
func (b *Bus) PublishWithSubject(ctx context.Context, data interface{}, subject string) error {
	meta, err := b.schemaRegistry.get(data)
	if err != nil {
		return err
	}
	msg, err := b.generateTransportMessage(meta, data)
	if err != nil {
		return err
	}
	msg.Subject = subject
	return b.publish(ctx, msg)
}

// PublishBulk Propagate multiple messages to the ecosystem.
func (b *Bus) PublishBulk(ctx context.Context, data ...interface{}) error {
	errs := new(multierror.Error)
	for _, d := range data {
		if err := b.Publish(ctx, d); err != nil {
			errs = multierror.Append(err, errs)
		}
	}
	return errs.ErrorOrNil()
}

func (b *Bus) generateTransportMessage(meta *MessageMetadata, data interface{}) (*TransportMessage, error) {
	msgID, err := b.Factories.IDFactory.NewID()
	if err != nil {
		return nil, err
	}

	encodedMsg, err := b.Marshaler.Marshal(data)
	if err != nil {
		return nil, err
	}

	return &TransportMessage{
		ID:              msgID,
		Source:          meta.Source,
		SpecVersion:     CloudEventsSpecVersion,
		Type:            meta.Topic,
		DataContentType: b.Marshaler.GetContentType(),
		DataSchema:      b.getDataSchema(meta),
		Time:            time.Now().UTC().Format(time.RFC3339),
		Topic:           meta.Topic,
		Data:            encodedMsg,
	}, nil
}

func (b *Bus) getDataSchema(meta *MessageMetadata) string {
	if meta.SchemaURI != "" {
		return meta.SchemaURI
	}
	return b.Configuration.RemoteSchemaRegistryURI
}

func (b *Bus) getSchemaVersion(meta MessageMetadata) int {
	if meta.SchemaVersion != 0 {
		return meta.SchemaVersion
	}
	return b.Configuration.MajorVersion
}

// PublishRaw Propagate a raw `Gluon` internal message to the ecosystem.
func (b *Bus) PublishRaw(ctx context.Context, msg *TransportMessage) error {
	return b.publish(ctx, msg)
}

func (b *Bus) publish(ctx context.Context, msg *TransportMessage) error {
	b.injectMessageContext(ctx, msg)
	var handlerFunc PublisherFunc
	handlerFunc = b.driver.Publish
	for _, mw := range b.publisherMiddleware {
		if mw != nil {
			handlerFunc = mw(handlerFunc)
		}
	}
	return handlerFunc(ctx, msg)
}

func (b *Bus) injectMessageContext(ctx context.Context, msg *TransportMessage) {
	if correlation, ok := ctx.Value(contextCorrelationID).(gluonContextKey); ok {
		msg.CorrelationID = string(correlation)
	} else {
		msg.CorrelationID = msg.ID
	}

	if causation, ok := ctx.Value(contextMessageID).(gluonContextKey); ok {
		msg.CausationID = string(causation)
	} else {
		msg.CausationID = msg.ID
	}
}

// GetSchemaMetadata retrieves metadata from the internal schema registry
func (b *Bus) GetSchemaMetadata(schema interface{}) (*MessageMetadata, error) {
	return b.schemaRegistry.get(schema)
}

// GetSchemaMetadataFromTopic retrieves metadata from the internal schema registry using the topic name
func (b *Bus) GetSchemaMetadataFromTopic(topic string) *MessageMetadata {
	return b.schemaRegistry.getByTopic(topic)
}

// Shutdown Close a Bus and its internal resources gracefully.
func (b *Bus) Shutdown(ctx context.Context) error {
	return b.driver.Shutdown(ctx)
}

func (b *Bus) isLoggerEnabled() bool {
	return b.Logger != nil
}
