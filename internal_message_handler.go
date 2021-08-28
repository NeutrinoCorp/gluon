package gluon

import (
	"context"
	"reflect"
)

// InternalMessageHandler is the message handler used by concrete drivers.
type InternalMessageHandler func(ctx context.Context, message *TransportMessage) error

func getInternalHandler(b *Bus) InternalMessageHandler {
	return func(ctx context.Context, msg *TransportMessage) error {
		subs := b.subscriberRegistry.get(msg.Topic)
		for _, sub := range subs {
			msgMeta := b.schemaRegistry.getByTopic(sub.key)
			data := reflect.New(msgMeta.schemaInternalType)
			err := b.Marshaler.Unmarshal(msg.Data, data.Interface())
			if err != nil && b.isLoggerEnabled() {
				b.Logger.Print("gluon: " + err.Error())
			}
			// TODO: Send to retry queue or dlq
			scopedCtx := injectCorrelationContext(ctx, msg)
			_ = sub.getHandler()(scopedCtx, &Message{
				Headers: generateHeaders(msg, sub),
				Data:    data.Elem().Interface(),
			})
		}
		return nil
	}
}

func injectCorrelationContext(ctx context.Context, msg *TransportMessage) context.Context {
	if msg.CorrelationID != "" {
		ctx = context.WithValue(ctx, contextCorrelationID, gluonContextKey(msg.CorrelationID))
	}
	ctx = context.WithValue(ctx, contextMessageID, gluonContextKey(msg.ID))
	return ctx
}
