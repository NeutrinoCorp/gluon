package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/neutrinocorp/gluon"
)

func main() {
	b := gluon.NewBroker()

	b.Topic("foo-event").Group("analytics-service").SubscriberFunc(func(ctx context.Context, e gluon.IntegrationEvent) error {
		return nil
	})

	b.Topic("bar-event").Group("organization-service").SubscriberFunc(func(ctx context.Context, e gluon.IntegrationEvent) error {
		return nil
	})

	b.Topic("bar-event").Group("payment-service").SubscriberFunc(func(ctx context.Context, e gluon.IntegrationEvent) error {
		return nil
	})

	b.Topic("baz-event").Group("user-service").SubscriberFunc(func(ctx context.Context, e gluon.IntegrationEvent) error {
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

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := b.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
