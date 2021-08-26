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
	messageRegistry    *messageRegistry
	subscriberRegistry *subscriberRegistry
	messageRouter      *messageScheduler
}

func NewBus(driver string) *Bus {
	return &Bus{
		Marshaler: defaultMarshaler,
		Factories: Factories{
			IDFactory: defaultIDFactory,
		},
		driver:             drivers[driver],
		messageRegistry:    newMessageRegistry(),
		subscriberRegistry: newSubscriberRegistry(),
		messageRouter:      newMessageScheduler(),
	}
}

func (b *Bus) RegisterMessage(meta MessageMetadata, msg interface{}) {
	b.messageRegistry.register(meta, msg)
}

func (b *Bus) ListenAndServe() error {
	b.driver.SetParentBus(b)
	b.messageRouter.setBus(b)
	b.driver.SetInternalHandler(b.messageRouter.getHandler())
	if b.BaseContext == nil {
		b.BaseContext = context.Background()
	}
	return b.driver.Start(b.BaseContext)
}

func (b *Bus) Subscribe(topic string, msg interface{}) *Subscriber {
	entry := newSubscriber(topic)
	b.subscriberRegistry.register(topic, entry)
	b.subscriberRegistry.registerType(topic, msg)
	b.driver.Subscribe(b.BaseContext, topic)
	return entry
}

// Publish Propagate a message to the ecosystem using the internal topic registry agent to generate the topic.
func (b *Bus) Publish(ctx context.Context, data interface{}) error {
	meta, err := b.messageRegistry.get(data)
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
	}

	encodedMsg, err := b.Marshaler.Marshal(data)
	if err != nil {
		return err
	}
	transportMessage.Data = encodedMsg
	return b.driver.Publish(ctx, meta.Topic, transportMessage)
}

// PublishRaw Propagate a raw `Gluon` internal message to the ecosystem.
func (b *Bus) PublishRaw(ctx context.Context, topic string, msg *TransportMessage) error {
	return b.driver.Publish(ctx, topic, msg)
}

func (b *Bus) getDataSchema(meta MessageMetadata) string {
	if meta.SchemaURI != "" {
		return meta.SchemaURI
	}
	return b.Configuration.SchemaRegistryURI
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
