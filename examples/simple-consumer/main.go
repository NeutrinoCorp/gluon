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
		gluon.WithDriverConfiguration(
			glocal.Configuration{
				IsDurable: true,
			}))
	return bus
}

func registerSchemas(bus *gluon.Bus) {
	bus.RegisterSchema(ItemPaid{},
		gluon.WithTopic("org.neutrino.marketplace.item.paid"),
		gluon.WithSource("https://api.neutrino.org/marketplace/items"))

	bus.RegisterSchema(OrderSent{},
		gluon.WithTopic("org.neutrino.warehouse.order.sent"),
		gluon.WithSource("https://api.neutrino.org/warehouse/orders"),
		gluon.WithSchemaDefinition("https://pubsub.neutrino.org/warehouse/schemas"),
		gluon.WithSchemaVersion(5))

	bus.RegisterSchema(OrderDelivered{},
		gluon.WithTopic("org.neutrino.warehouse.order.delivered"),
		gluon.WithSource("https://api.neutrino.org/warehouse/orders"),
		gluon.WithSchemaDefinition("https://pubsub.neutrino.org/warehouse/schemas"))
}

func registerSubscribers(bus *gluon.Bus) {
	bus.Subscribe(ItemPaid{}).HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
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

	bus.Subscribe(OrderSent{}).HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
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

	bus.Subscribe(OrderDelivered{}).HandlerFunc(logMiddleware(func(ctx context.Context, message *gluon.Message) error {
		event := message.Data.(OrderDelivered)
		log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Order %s has been delivered at %s", event.OrderID,
			event.DeliveredAt.Format(time.RFC3339))
		log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
			message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
		return nil
	}))
}

func publishMessage(bus *gluon.Bus) {
	rootCtx := context.TODO()
	err := bus.PublishWithTopic(rootCtx, "org.neutrino.marketplace.item.paid", ItemPaid{
		ItemID:   uuid.NewString(),
		Total:    99.99,
		Quantity: 2,
		PaidAt:   time.Now().UTC(),
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
