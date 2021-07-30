package main

import (
	"context"
	"errors"
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
		log.Printf("stdout::Logger:consumer:%s\n", msg.Data)
		return next.Handle(ctx, msg)
	})
}

var onUserCreatedAnalytics gluon.SubscriberFunc = func(ctx context.Context, msg gluon.Message) error {
	_, ok := msg.Data.(UserCreated)
	if !ok {
		log.Print("stdout::Logger:analytics: cannot cast at analytics service")
		return errors.New("cannot cast incoming data")
	}
	log.Print("stdout::Logger:analytics: user received at analytics service")
	return nil
}

func main() {
	b := gluon.NewBroker("memory",
		gluon.WithOrganization("neutrino"),
		gluon.WithMajorVersion(2),
		gluon.WithService("iam"),
		gluon.WithSource("org.neutrinocorp"),
		gluon.WithSchemaRegistry("https://event-api.neutrinocorp.org/schemas"),
		gluon.WithMarshaller(gluon.JSONMarshaller{}))
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

	topic := gluon.GenerateEventTopic(b, UserCreated{})

	go func() {
		_, _ = b.Publish(context.Background(), topic, UserCreated{
			UserID: "1",
		})
	}()

	go func() {
		_, _ = b.Publish(context.Background(), topic, UserCreated{
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
