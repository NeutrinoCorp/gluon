package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/rs/zerolog"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/aws/aws-sdk-go-v2/service/glue"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/google/uuid"
	"github.com/neutrinocorp/gluon"
	"github.com/neutrinocorp/gluon/gaws"
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
		log.Printf("[TOPIC-%s] producing message", message.Topic)
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
	// use Localstack
	awsEndpoint := "http://localhost:4566"
	awsRegion := "us-east-1"

	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	cfg, _ := config.LoadDefaultConfig(context.TODO())
	bus := gluon.NewBus("aws_sns_sqs",
		gluon.WithSchemaRegistry(gaws.GlueSchemaRegistry{
			Client:           glue.NewFromConfig(cfg),
			RegistryName:     "sample",
			UseLatestVersion: true,
		}),
		gluon.WithMajorVersion(2),
		gluon.WithLogger(logger),
		gluon.WithPublisherMiddleware(logProducerMiddleware),
		gluon.WithConsumerMiddleware(logMiddleware),
		gluon.WithMarshaler(gluon.NewMarshalerAvro()),
		gluon.WithConsumerGroup("ncorp-places-marketplace-prod-1"),
		gluon.WithDriverConfiguration(gaws.SnsSqsConfig{
			AwsConfig: cfg,
			SnsClient: sns.NewFromConfig(cfg, func(options *sns.Options) {
				options.EndpointResolver = sns.EndpointResolverFunc(func(region string, options sns.EndpointResolverOptions) (aws.Endpoint, error) {
					return aws.Endpoint{
						PartitionID:   "aws",
						URL:           awsEndpoint,
						SigningRegion: awsRegion,
					}, nil
				})
			}),
			SqsClient: sqs.NewFromConfig(cfg, func(options *sqs.Options) {
				options.EndpointResolver = sqs.EndpointResolverFunc(func(region string, options sqs.EndpointResolverOptions) (aws.Endpoint, error) {
					return aws.Endpoint{
						PartitionID:   "aws",
						URL:           awsEndpoint,
						SigningRegion: awsRegion,
					}, nil
				})
			}),
			CustomSqsEndpoint:    awsEndpoint,
			AccountID:            "000000000000",
			FailedPollingBackoff: time.Second * 5,
		}))
	return bus
}

func registerSchemas(bus *gluon.Bus) {
	bus.RegisterSchema(ItemPaid{},
		gluon.WithTopic("ncorp.places.marketplace.prod.2.event.item.paid"),
		gluon.WithSchemaName("item_paid"),
		gluon.WithSource("https://api.neutrino.org/marketplace/items"))

	bus.RegisterSchema(OrderSent{},
		gluon.WithTopic("ncorp.places.warehouse.prod.1.event.package.sent"),
		gluon.WithSource("https://api.neutrino.org/warehouse/orders"),
		gluon.WithSchemaName("order_sent"),
		gluon.WithSchemaVersion(5))

	bus.RegisterSchema(OrderDelivered{},
		gluon.WithTopic("ncorp.places.warehouse.prod.1.event.package.delivered"),
		gluon.WithSource("https://api.neutrino.org/warehouse/orders"),
		gluon.WithSchemaName("order_delivered"))
}

func registerSubscribers(bus *gluon.Bus) {
	bus.Subscribe(ItemPaid{}).
		Group("ncorp-places-warehouse-prod-1-send_order-on-item_paid").
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
		Group("ncorp-places-warehouse-prod-1-deliver_order-on-order_sent").
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
		Group("ncorp-places-analytics-prod-1-ingest_data-on-order_delivered").
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
			return errors.New("send to DLQ on 3rd retry")
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
