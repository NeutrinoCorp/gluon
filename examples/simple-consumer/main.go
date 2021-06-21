package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/neutrinocorp/gluon"
	_ "github.com/neutrinocorp/gluon/gmemory"
)

type UserCreated struct {
	UserID string `json:"user_id"`
}

var _ gluon.Event = UserCreated{}

func (e UserCreated) Source() string {
	return "/foo"
}

func (e UserCreated) Subject() string {
	return ""
}

func (e UserCreated) Topic() string {
	return "neutrinocorp.user.event.user.created"
}

type PublisherLogger struct {
	Next gluon.Publisher
}

var _ gluon.Publisher = PublisherLogger{}

func (p PublisherLogger) PublishMessage(ctx context.Context, msg *gluon.Message) error {
	log.Printf("stdout::Logger:publisher:%+v\n", msg)
	return p.Next.PublishMessage(ctx, msg)
}

func logMiddleware(next gluon.Subscriber) gluon.Subscriber {
	return gluon.SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
		log.Printf("stdout::Logger:consumer:%+v\n", msg)
		return next.Handle(ctx, msg)
	})
}

var onUserCreatedAnalytics gluon.SubscriberFunc = func(ctx context.Context, msg gluon.Message) error {
	e := UserCreated{}
	err := json.Unmarshal(msg.Data.([]byte), &e)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	b := gluon.NewBroker("memory",
		gluon.WithSource("org.neutrinocorp/user"),
		gluon.WithSchemaRegistry("https://event-api.neutrinocorp.org/schemas"),
		gluon.WithMarshaler(gluon.JSONMarshaler{}))
	p := PublisherLogger{
		Next: b.Publisher,
	}
	b.Publisher = p

	event := UserCreated{}

	b.Event(event).Group("analytics-service").Subscriber(logMiddleware(onUserCreatedAnalytics))

	b.Event(event).Group("organization-service").SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
		log.Print("received user created at org service")
		return nil
	})

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		if err := b.ListenAndServe(); err != nil && err != gluon.ErrBrokerClosed {
			log.Fatal(err)
		}
	}()

	go func() {
		b.Publish(context.Background(), UserCreated{
			UserID: "1",
		})
	}()

	go func() {
		b.Publish(context.Background(), UserCreated{
			UserID: "2",
		})
	}()

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := b.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
