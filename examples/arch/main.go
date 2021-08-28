package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"

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

type OrderSent struct {
	OrderID string    `json:"order_id"`
	SentAt  time.Time `json:"sent_at"`
}

type OrderDelivered struct {
	OrderID     string    `json:"order_id"`
	DeliveredAt time.Time `json:"delivered_at"`
}

func main() {
	bus := newBus()

	registerSchemas(bus)
	registerSubscribers(bus)
	go func() {
		if err := bus.ListenAndServe(); err != nil && err != gluon.ErrBrokerClosed {
			log.Fatal(err)
		}
	}()

	// TODO: implement both, volatile (in memory) and durable storage (on disk) local implementation.
	// TODO: use Apache Parquet + gzip to store partitioned data per-topic on disk (Apache Kafka like).

	// TODO: implement either Confluent or AWS Glue schemas registries for both producer and consumer.

	// TODO: add heterogeneous subscribers and publishers, use a global pub/sub and for-each entry
	// TODO: create a util to generate topics using the message type, reversed org DNS and service/domain_ctx (and service major version)

	rootCtx := context.TODO()
	err := bus.Publish(rootCtx, ItemPaid{
		ItemID:   uuid.NewString(),
		Total:    99.99,
		Quantity: 2,
		PaidAt:   time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}

	gracefulShutdown(bus)
}

func newBus() *arch.Bus {
	bus := arch.NewBus("local")
	bus.Configuration.RemoteSchemaRegistryURI = "https://pubsub.neutrino.org/marketplace/schemas"
	bus.Configuration.MajorVersion = 2
	bus.Configuration.Driver = glocal.Configuration{IsDurable: true}
	return bus
}

func registerSchemas(bus *arch.Bus) {
	bus.RegisterSchema(ItemPaid{}, arch.MessageMetadata{
		Topic:  "org.neutrino.marketplace.item.paid",
		Source: "https://api.neutrino.org/marketplace/items",
	})

	bus.RegisterSchema(OrderSent{}, arch.MessageMetadata{
		Topic:     "org.neutrino.warehouse.order.sent",
		Source:    "https://api.neutrino.org/warehouse/orders",
		SchemaURI: "https://pubsub.neutrino.org/warehouse/schemas",
	})

	bus.RegisterSchema(OrderDelivered{}, arch.MessageMetadata{
		Topic:     "org.neutrino.warehouse.order.delivered",
		Source:    "https://api.neutrino.org/warehouse/orders",
		SchemaURI: "https://pubsub.neutrino.org/warehouse/schemas",
	})
}

func registerSubscribers(bus *arch.Bus) {
	bus.Subscribe(ItemPaid{}).HandlerFunc(func(ctx context.Context, message *arch.Message) error {
		event := message.Data.(ItemPaid)
		log.Printf("[WAREHOUSE_SERVICE] | Item %s has been paid at %s, total was %f", event.ItemID,
			event.PaidAt.Format(time.RFC3339), event.Total)
		log.Printf("[WAREHOUSE_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
			message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
		return bus.Publish(ctx, OrderSent{
			OrderID: uuid.NewString(),
			SentAt:  time.Now().UTC(),
		})
	})

	bus.Subscribe(OrderSent{}).HandlerFunc(func(ctx context.Context, message *arch.Message) error {
		event := message.Data.(OrderSent)
		log.Printf("[WAREHOUSE_SERVICE] | Order %s has been sent at %s", event.OrderID,
			event.SentAt.Format(time.RFC3339))
		log.Printf("[WAREHOUSE_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
			message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
		return bus.Publish(ctx, OrderDelivered{
			OrderID:     uuid.NewString(),
			DeliveredAt: time.Now().UTC(),
		})
	})

	bus.Subscribe(OrderDelivered{}).HandlerFunc(func(ctx context.Context, message *arch.Message) error {
		event := message.Data.(OrderDelivered)
		log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Order %s has been delivered at %s", event.OrderID,
			event.DeliveredAt.Format(time.RFC3339))
		log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
			message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
		return nil
	})
}

func gracefulShutdown(bus *arch.Bus) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := bus.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
