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
		logInternalConsumerError(b, err)
		if err != nil && b.isLoggerEnabled() {
			return err
		}
		return execConsumer(ctx, b, sub, msg, data)
	}
}

func logInternalConsumerError(b *Bus, err error) {
	if err != nil && b.isLoggerEnabled() {
		b.Logger.Print("gluon: " + err.Error())
	}
}

func execConsumer(ctx context.Context, b *Bus, sub *Subscriber, msg *TransportMessage, data reflect.Value) error {
	scopedCtx := injectCorrelationContext(ctx, msg)
	handlerFunc := sub.GetHandlerFunc()
	for _, mw := range b.consumerMiddleware {
		if mw != nil {
			handlerFunc = mw(handlerFunc)
		}
	}
	return handlerFunc(scopedCtx, &Message{
		Headers: generateHeaders(msg, sub),
		Data:    data.Elem().Interface(),
	})
}

func injectCorrelationContext(ctx context.Context, msg *TransportMessage) context.Context {
	if msg.CorrelationID != "" {
		ctx = context.WithValue(ctx, contextCorrelationID, gluonContextKey(msg.CorrelationID))
	}
	ctx = context.WithValue(ctx, contextMessageID, gluonContextKey(msg.ID))
	return ctx
}
