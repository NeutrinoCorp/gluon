package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
	"github.com/neutrinocorp/gluon"
	_ "github.com/neutrinocorp/gluon/gkafka"
)

type ItemPaid struct {
	ItemID   string    `avro:"item_id"`
	Total    float32   `avro:"total"`
	Quantity int       `avro:"quantity"`
	PaidAt   time.Time `avro:"paid_at"`
}

type OrderSent struct {
	OrderID string    `avro:"order_id"`
	SentAt  time.Time `avro:"sent_at"`
}

type OrderDelivered struct {
	OrderID     string    `avro:"order_id"`
	DeliveredAt time.Time `avro:"delivered_at"`
}

func logMiddleware(next gluon.HandlerFunc) gluon.HandlerFunc {
	return func(ctx context.Context, msg *gluon.Message) error {
		log.Printf("stdout::Logger:consumer:%+v\n", msg)
		log.Printf("stdout::Logger:consumer:%s\n", msg.Data)
		return next(ctx, msg)
	}
}

func logProducerMiddleware(next gluon.PublisherFunc) gluon.PublisherFunc {
	return func(ctx context.Context, message *gluon.TransportMessage) error {
		log.Print("producing message")
		return next(ctx, message)
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
	logger := log.New(os.Stdout, "", 0)
	bus := gluon.NewBus("kafka",
		gluon.WithCluster("localhost:9092", "localhost:9093", "localhost:9094"),
		gluon.WithRemoteSchemaRegistry("https://pubsub.neutrino.org/marketplace/schemas"),
		gluon.WithMajorVersion(2),
		gluon.WithLogger(logger),
		gluon.WithMarshaler(gluon.NewMarshalerAvro()),
		gluon.WithPublisherMiddleware(logProducerMiddleware),
		gluon.WithConsumerMiddleware(logMiddleware))
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
	bus.Subscribe(ItemPaid{}).
		Group("warehouse-service").
		HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
			event := message.Data.(ItemPaid)
			log.Printf("[WAREHOUSE_SERVICE] | Item %s has been paid at %s, total was %f", event.ItemID,
				event.PaidAt.Format(time.RFC3339), event.Total)
			log.Printf("[WAREHOUSE_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
				message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
			return bus.PublishWithSubject(ctx, OrderSent{
				OrderID: uuid.NewString(),
				SentAt:  time.Now().UTC(),
			}, event.ItemID)
		})

	bus.Subscribe(OrderSent{}).
		Group("warehouse-service").
		HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
			event := message.Data.(OrderSent)
			log.Printf("[WAREHOUSE_SERVICE] | Order %s has been sent at %s", event.OrderID,
				event.SentAt.Format(time.RFC3339))
			log.Printf("[WAREHOUSE_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
				message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
			return bus.Publish(ctx, OrderDelivered{
				OrderID:     event.OrderID,
				DeliveredAt: time.Now().UTC(),
			})
		})

	bus.Subscribe(OrderDelivered{}).
		Group("customer-analytics-service").
		HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
			event := message.Data.(OrderDelivered)
			log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Order %s has been delivered at %s", event.OrderID,
				event.DeliveredAt.Format(time.RFC3339))
			log.Printf("[CUSTOMER_ANALYTICS_SERVICE] | Message metadata: id: %s, correlation: %s, causation: %s",
				message.GetMessageID(), message.GetCorrelationID(), message.GetCausationID())
			return nil
		})

	bus.Subscribe(OrderDelivered{}).
		HandlerFunc(func(ctx context.Context, message *gluon.Message) error {
			log.Print("[SHARDED_CONSUMER] Order has been delivered")
			return nil
		})
}

func publishMessage(bus *gluon.Bus) {
	time.Sleep(time.Second * 5) // wait for cold boot
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
