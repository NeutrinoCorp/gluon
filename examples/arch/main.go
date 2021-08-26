package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/neutrinocorp/gluon"

	"github.com/neutrinocorp/gluon/arch"
	"github.com/neutrinocorp/gluon/arch/glocal"
)

type ItemPaid struct {
	ItemID   string    `json:"item_id"`
	Total    float64   `json:"total"`
	Quantity int       `json:"quantity"`
	PaidAt   time.Time `json:"paid_at"`
}

type UserSignedUp struct {
	UserID     string    `json:"user_id"`
	Username   string    `json:"username"`
	SignedInAt time.Time `json:"signed_in_at"`
}

func main() {
	bus := arch.NewBus("local")
	bus.Configuration.SchemaRegistryURI = "https://pubsub.neutrino.org/marketplace/schemas"
	bus.Configuration.MajorVersion = 2
	bus.Configuration.Driver = glocal.Configuration{IsDurable: true}
	ctx := context.TODO()

	bus.Subscribe("org.neutrino.iam.user.signed_up.v2", ItemPaid{}).
		HandlerFunc(func(ctx context.Context, msg *arch.Message) error {
			e := msg.Data.(*ItemPaid)
			log.Printf("%+v", e)
			return nil // return nil to Ack, any error to NAck, middleware feature could enable logging, metrics, etc...
		})

	bus.Subscribe("org.neutrino.iam.user.signed_up.v2", ItemPaid{}).Group("service-b").
		HandlerFunc(func(ctx context.Context, msg *arch.Message) error {
			log.Print(msg.GetConsumerGroup())
			return nil // return nil to Ack, any error to NAck, middleware feature could enable logging, metrics, etc...
		})

	go func() {
		if err := bus.ListenAndServe(); err != nil && err != gluon.ErrBrokerClosed {
			log.Fatal(err)
		}
	}()

	event := ItemPaid{
		ItemID:   "abc",
		Total:    99.99,
		Quantity: 2,
		PaidAt:   time.Now().UTC(),
	}

	// TODO: implement both, volatile (in memory) and durable storage (on disk) local implementation.
	// TODO: use Apache Parquet + gzip to store partitioned data per-topic on disk (Apache Kafka like).

	// TODO: implement either Confluent or AWS Glue schemas registries for both producer and consumer.

	// TODO: add heterogeneous subscribers and publishers, use a global pub/sub and for-each entry
	// TODO: create a util to generate topics using the message type, reversed org DNS and service/domain_ctx (and service major version)

	bus.RegisterMessage(arch.MessageMetadata{
		Topic:         "org.neutrino.iam.user.signed_up",
		Source:        "https://api.neutrino.org/marketplace/items",
		SchemaURI:     "",
		SchemaVersion: 0,
	}, event)

	err := bus.Publish(ctx, event)
	if err != nil {
		panic(err)
	}

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err = bus.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
