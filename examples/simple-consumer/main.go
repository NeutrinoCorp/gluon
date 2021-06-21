package main

import (
	"context"
	"fmt"
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

func (e UserCreated) Schema() string {
	return "https://event-api.neutrinocorp.org/schemas"
}

func (e UserCreated) Topic() string {
	return "neutrinocorp.user.event.user.created"
}

func main() {
	b := gluon.NewBroker("memory",
		gluon.WithSource("org.neutrinocorp/cosmos/user"))

	b.Event(UserCreated{}).Group("analytics-service").SubscriberFunc(func(ctx context.Context, msg gluon.Message) error {
		fmt.Printf("msg: %+v\n", msg)
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
		b.Publish(context.Background(), UserCreated{
			UserID: "1",
		})
	}()

	go func() {
		b.Publisher.PublishMessage(context.Background(), &gluon.Message{
			Type: "bar-event",
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
