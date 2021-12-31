package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/neutrinocorp/gluon"
	"github.com/neutrinocorp/gluon/gaws"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ItemPaid struct {
	ItemID   string    `json:"item_id"`
	Total    float64   `json:"total"`
	Quantity int       `json:"quantity"`
	PaidAt   time.Time `json:"paid_at"`
}

func logMiddleware(next gluon.HandlerFunc) gluon.HandlerFunc {
	return func(ctx context.Context, msg *gluon.Message) error {
		log.Printf("stdout::Logger:consumer:%+v\n", msg)
		log.Printf("stdout::Logger:consumer:%s\n", msg.Data)
		return next(ctx, msg)
	}
}

func main() {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	cfg, _ := config.LoadDefaultConfig(context.TODO())
	bus := gluon.NewBus("aws_sns_sqs",
		gluon.WithLogger(logger),
		gluon.WithConsumerMiddleware(logMiddleware),
		gluon.WithDriverConfiguration(gaws.SnsSqsConfig{
			AwsConfig:                 cfg,
			AccountID:                 "228850758643",
			SnsClient:                 sns.NewFromConfig(cfg),
			SqsClient:                 sqs.NewFromConfig(cfg),
			CustomSqsEndpoint:         "",
			MaxNumberOfMessagesPolled: 0,
			VisibilityTimeout:         0,
			WaitTimeSeconds:           0,
			MaxBatchPollingRetries:    3, // leave this as 0 if it is desired to keep workers running on failing scenarios
			FailedPollingBackoff:      time.Second * 3,
		}))
	registerEventSchemas(bus)
	subscribeToMessages(bus)
	go func() {
		if err := bus.ListenAndServe(); err != nil && err != gluon.ErrBusClosed {
			log.Fatal().Msg(err.Error())
		}
	}()
	go publishMessage(bus)
	gracefulShutdown(bus)
}

func registerEventSchemas(bus *gluon.Bus) {
	bus.RegisterSchema(ItemPaid{},
		gluon.WithTopic("ncorp.places.marketplace.prod.2.event.item.paid"),
		gluon.WithSource("ncorp-places-marketplace-prod"),
		gluon.WithSchemaName("https://places.neutrinocorp.org/engineering/docs/apis/streams#ItemPaid"))
}

func subscribeToMessages(bus *gluon.Bus) {
	bus.Subscribe(ItemPaid{}).
		Group("ncorp.wallet.core.prod.2.add_transaction.on.item_paid").
		HandlerFunc(func(ctx context.Context, msg *gluon.Message) error {
			log.Print(msg.Data)
			return nil
		})
}

func publishMessage(bus *gluon.Bus) {
	time.Sleep(time.Second * 5) // wait for cold boot
	rootCtx := context.TODO()
	itemId := uuid.NewString()
	err := bus.PublishWithSubject(rootCtx, ItemPaid{
		ItemID:   itemId,
		Total:    99.99,
		Quantity: 2,
		PaidAt:   time.Now().UTC(),
	}, itemId)
	if err != nil {
		log.Error().Msg(err.Error())
	}
}

func gracefulShutdown(bus *gluon.Bus) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := bus.Shutdown(ctx); err != nil {
		log.Fatal().Msg(err.Error())
	}
}
