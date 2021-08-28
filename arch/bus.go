package arch

import (
	"context"
	"log"
	"strconv"
	"time"
)

// Bus Is a facade component used to interact with `Gluon`.
type Bus struct {
	Marshaler     Marshaler
	Factories     Factories
	Configuration BusConfiguration
	BaseContext   context.Context
	Logger        *log.Logger

	driver             Driver
	schemaRegistry     *schemaRegistry
	subscriberRegistry *subscriberRegistry
}

// NewBus Allocate a
func NewBus(driver string) *Bus {
	return &Bus{
		Marshaler: defaultMarshaler,
		Factories: Factories{
			IDFactory: defaultIDFactory,
		},
		driver:             drivers[driver],
		schemaRegistry:     newSchemaRegistry(),
		subscriberRegistry: newSubscriberRegistry(),
	}
}

func (b *Bus) RegisterSchema(schema interface{}, meta MessageMetadata) {
	b.schemaRegistry.register(schema, meta)
}

func (b *Bus) ListenAndServe() error {
	b.driver.SetParentBus(b)
	b.driver.SetInternalHandler(getInternalHandler(b))
	if b.BaseContext == nil {
		b.BaseContext = context.Background()
	}
	return b.driver.Start(b.BaseContext)
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
	b.driver.Subscribe(b.BaseContext, meta.Topic)
	return entry
}

// SubscribeTopic Set a subscription task using a raw topic name.
func (b *Bus) SubscribeTopic(topic string) *Subscriber {
	entry := newSubscriber(topic)
	b.subscriberRegistry.register(topic, entry)
	b.driver.Subscribe(b.BaseContext, topic)
	return entry
}

// ListSubscribersFromTopic Get the subscription task queue of a registered topic.
func (b *Bus) ListSubscribersFromTopic(t string) []*Subscriber {
	return b.subscriberRegistry.get(t)
}

// Publish Propagate a message to the ecosystem using the internal topic registry agent to generate the topic.
func (b *Bus) Publish(ctx context.Context, data interface{}) error {
	meta, err := b.schemaRegistry.get(data)
	if err != nil {
		return err
	}

	msgID, err := b.Factories.IDFactory.NewID()
	if err != nil {
		return err
	}

	transportMessage := &TransportMessage{
		ID:              msgID,
		Source:          meta.Source,
		SpecVersion:     CloudEventsSpecVersion,
		Type:            meta.Topic + ".v" + strconv.Itoa(b.getSchemaVersion(meta)),
		DataContentType: b.Marshaler.GetContentType(),
		DataSchema:      b.getDataSchema(meta),
		Time:            time.Now().UTC().Format(time.RFC3339),
		Topic:           meta.Topic,
	}

	b.injectContextToMessage(ctx, transportMessage)
	encodedMsg, err := b.Marshaler.Marshal(data)
	if err != nil {
		return err
	}
	transportMessage.Data = encodedMsg
	return b.driver.Publish(ctx, meta.Topic, transportMessage)
}

func (b *Bus) injectContextToMessage(ctx context.Context, msg *TransportMessage) {
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

// PublishRaw Propagate a raw `Gluon` internal message to the ecosystem.
func (b *Bus) PublishRaw(ctx context.Context, topic string, msg *TransportMessage) error {
	return b.driver.Publish(ctx, topic, msg)
}

func (b *Bus) getDataSchema(meta MessageMetadata) string {
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

func (b *Bus) Shutdown(ctx context.Context) error {
	return b.driver.Shutdown(ctx)
}

func (b *Bus) isLoggerEnabled() bool {
	return b.Logger != nil && b.Configuration.EnableLogging
}
