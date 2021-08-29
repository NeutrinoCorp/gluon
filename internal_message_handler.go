package gluon

import (
	"context"
	"reflect"
)

// InternalMessageHandler is the message handler used by concrete drivers.
type InternalMessageHandler func(ctx context.Context, subscriber *Subscriber, message *TransportMessage) error

func getInternalHandler(b *Bus) InternalMessageHandler {
	return func(ctx context.Context, sub *Subscriber, msg *TransportMessage) error {
		msgMeta := b.schemaRegistry.getByTopic(sub.key)
		data := reflect.New(msgMeta.schemaInternalType)
		err := b.Marshaler.Unmarshal(msg.Data, data.Interface())
		if err != nil && b.isLoggerEnabled() {
			b.Logger.Print("gluon: " + err.Error())
			return err
		}
		// TODO: Enable Ack mechanism (consumer id passed through InternalHandler params?)
		// TODO: Send to retry queue or dlq
		scopedCtx := injectCorrelationContext(ctx, msg)
		return sub.GetDefaultHandler()(scopedCtx, &Message{
			Headers: generateHeaders(msg, sub),
			Data:    data.Elem().Interface(),
		})
	}
}

func injectCorrelationContext(ctx context.Context, msg *TransportMessage) context.Context {
	if msg.CorrelationID != "" {
		ctx = context.WithValue(ctx, contextCorrelationID, gluonContextKey(msg.CorrelationID))
	}
	ctx = context.WithValue(ctx, contextMessageID, gluonContextKey(msg.ID))
	return ctx
}
