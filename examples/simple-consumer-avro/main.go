package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
	"github.com/neutrinocorp/gluon"
	"github.com/neutrinocorp/gluon/glocal"
	_ "github.com/neutrinocorp/gluon/glocal"
)

type ItemPaid struct {
	ItemID   string  `avro:"item_id"`
	Total    float32 `avro:"total"`
	Quantity int     `avro:"quantity"`
	PaidAt   string  `avro:"paid_at"`
}

type OrderSent struct {
	OrderID string `avro:"order_id"`
	SentAt  string `avro:"sent_at"`
}

type OrderDelivered struct {
	OrderID     string `avro:"order_id"`
	DeliveredAt string `avro:"delivered_at"`
}

func logMiddleware(next gluon.HandlerFunc) gluon.HandlerFunc {
	return func(ctx context.Context, msg *gluon.Message) error {
		log.Printf("stdout::Logger:consumer:%+v\n", msg)
		log.Printf("stdout::Logger:consumer:%s\n", msg.Data)
		return next(ctx, msg)
	}
}

func main() {
	bus := newBus()
	registerSchemas(bus)
	registerSubscribers(bus)
	go func() {
		if err := bus.ListenAndServe(); err != nil && err != gluon.ErrBusClosed {
			log.Fatal(err)
		}
	}()
	go publishMessage(bus)
	gracefulShutdown(bus)
}

func newBus() *gluon.Bus {
	bus := gluon.NewBus("local",
		gluon.WithRemoteSchemaRegistry("https://pubsub.neutrino.org/marketplace/schemas"),
		gluon.WithMajorVersion(2),
		gluon.WithMarshaler(gluon.NewMarshalerAvro()),
		gluon.WithDriverConfiguration(
			glocal.Configuration{
				IsDurable: true,
			}))
	return bus
}

func registerSchemas(bus *gluon.Bus) {
	bus.RegisterSchema(ItemPaid{},
		gluon.WithTopic("org.neutrino.marketplace.item.paid"),
		gluon.WithSchemaDefinition("./testdata/item_paid.avsc"),
		gluon.WithSource("https://api.neutrino.org/marketplace/items"))

	bus.RegisterSchema(OrderSent{},
		gluon.WithTopic("org.neutrino.warehouse.order.sent"),
		gluon.WithSource("https://api.neutrino.org/warehouse/orders"),
		gluon.WithSchemaDefinition("./testdata/order_sent.avsc"),
		gluon.WithSchemaVersion(5))

	bus.RegisterSchema(OrderDelivered{},
		gluon.WithTopic("org.neutrino.warehouse.order.delivered"),
		gluon.WithSource("https://api.neutrino.org/warehouse/orders"),
		gluon.WithSchemaDefinition("./testdata/order_delivered.avsc"))
}

func registerSubscribers(bus *gluon.Bus) {
	bus.Subscribe(ItemPaid{}).HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
		event := message.Data.(ItemPaid)
		log.Printf("[WAREHOUSE_SERVICE] | Item %s has been paid at %s, total was %f", event.ItemID,
			event.PaidAt, event.Total)
		// log.Printf("[WAREHOUSE_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
		// message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
		return bus.Publish(ctx, OrderSent{
			OrderID: uuid.NewString(),
			SentAt:  time.Now().UTC().Format(time.RFC3339),
		})
	})

	bus.Subscribe(OrderSent{}).HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
		event := message.Data.(OrderSent)
		log.Printf("[WAREHOUSE_SERVICE] | Order %s has been sent at %s", event.OrderID,
			event.SentAt)
		// log.Printf("[WAREHOUSE_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
		// message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
		return bus.Publish(ctx, OrderDelivered{
			OrderID:     event.OrderID,
			DeliveredAt: time.Now().UTC().Format(time.RFC3339),
		})
	})

	bus.Subscribe(OrderDelivered{}).HandlerFunc(logMiddleware(func(ctx context.Context, message *gluon.Message) error {
		event := message.Data.(OrderDelivered)
		log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Order %s has been delivered at %s", event.OrderID,
			event.DeliveredAt)
		// log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
		// message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
		return nil
	}))
}

func publishMessage(bus *gluon.Bus) {
	rootCtx := context.TODO()
	err := bus.Publish(rootCtx, ItemPaid{
		ItemID:   uuid.NewString(),
		Total:    99.99,
		Quantity: 2,
		PaidAt:   time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		panic(err)
	}
}

func gracefulShutdown(bus *gluon.Bus) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := bus.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
