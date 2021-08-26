package arch

import (
	"context"
	"reflect"
)

type messageScheduler struct {
	parentBus *Bus
}

func newMessageScheduler() *messageScheduler {
	return &messageScheduler{}
}

func (r *messageScheduler) setBus(b *Bus) {
	r.parentBus = b
}

func (r *messageScheduler) getHandler() InternalMessageHandler {
	return func(ctx context.Context, msg *TransportMessage) {
		subs := r.parentBus.subscriberRegistry.get(msg.Type)
		for _, s := range subs {
			go func(sub *Subscriber) {
				msgType := r.parentBus.subscriberRegistry.getType(sub.key)
				data := reflect.New(msgType).Interface()
				err := r.parentBus.Marshaler.Unmarshal(msg.Data, &data)
				if err != nil && r.parentBus.isLoggerEnabled() {
					r.parentBus.Logger.Print("gluon: " + err.Error())
				}
				_ = sub.getHandler()(ctx, &Message{
					Headers: generateHeaders(msg, sub),
					Data:    data,
				})
			}(s)
		}
	}
}
