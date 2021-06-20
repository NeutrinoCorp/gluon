package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/neutrinocorp/gluon"
	_ "github.com/neutrinocorp/gluon/gmemory"
)

func main() {
	b := gluon.NewBroker("memory", "user-service")

	b.Topic("foo-event").Group("analytics-service").SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
		log.Print(msg.Type)
		return nil
	})

	b.Topic("bar-event").Group("organization-service").SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
		log.Print(msg.Type)
		return nil
	})

	b.Topic("bar-event").Group("payment-service").SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
		log.Print(msg.Type)
		return nil
	})

	b.Topic("baz-event").Group("user-service").SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
		log.Print(msg.Type)
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
		b.Publisher.PublishMessage(context.Background(), &gluon.Message{
			Type: "bar-event",
		})
	}()

	go func() {
		b.Publisher.PublishMessage(context.Background(), &gluon.Message{
			Type: "foo-event",
		})
	}()

	go func() {
		b.Publisher.PublishMessage(context.Background(), &gluon.Message{
			Type: "baz-event",
		})
	}()

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := b.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
