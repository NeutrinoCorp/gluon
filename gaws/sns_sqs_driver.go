package gaws

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/neutrinocorp/gluon"
)

// Topic-queue chaining implementation
// For more info: https://aws.amazon.com/blogs/compute/application-integration-patterns-for-microservices-fan-out-strategies/
type snsSqsDriver struct {
	parentBus      *gluon.Bus
	messageHandler gluon.InternalMessageHandler
	config         SnsSqsConfig
	snsClient      *sns.Client
	sqsClient      *sqs.Client

	subscriberWorkerPool sync.Pool
	subscriberWorkers    []*snsSqsSubscriptionWorker
}

var (
	_                     gluon.Driver = &snsSqsDriver{}
	defaultDriver         *snsSqsDriver
	snsSqsDriverSingleton = sync.Once{}
)

func init() {
	snsSqsDriverSingleton.Do(func() {
		defaultDriver = &snsSqsDriver{}
		defaultDriver.subscriberWorkerPool = sync.Pool{New: func() interface{} {
			return newSnsSqsSubscriptionWorker(defaultDriver)
		}}
		gluon.Register("aws_sns_sqs", defaultDriver)
	})
}

func (d *snsSqsDriver) SetParentBus(b *gluon.Bus) {
	d.parentBus = b
	if cfg, ok := b.Configuration.Driver.(SnsSqsConfig); ok {
		d.config = cfg
		d.snsClient = sns.NewFromConfig(cfg.AwsConfig)
		d.sqsClient = sqs.NewFromConfig(cfg.AwsConfig)

	}
}

func (d *snsSqsDriver) SetInternalHandler(h gluon.InternalMessageHandler) {
	d.messageHandler = h
}

func (d *snsSqsDriver) Start(ctx context.Context) error {
	return nil
}

func (d *snsSqsDriver) Shutdown(_ context.Context) error {
	for _, w := range d.subscriberWorkers {
		d.subscriberWorkerPool.Put(w)
	}
	d.subscriberWorkers = nil
	return nil
}

func (d *snsSqsDriver) Subscribe(ctx context.Context, subscriber *gluon.Subscriber) error {
	w := d.subscriberWorkerPool.Get().(*snsSqsSubscriptionWorker)
	d.subscriberWorkers = append(d.subscriberWorkers, w)
	return w.start(ctx, subscriber)
}

func (d *snsSqsDriver) Publish(ctx context.Context, message *gluon.TransportMessage) error {
	snsMsg, err := marshalSnsMessage(message)
	if err != nil {
		return err
	}
	_, err = d.snsClient.Publish(ctx, &sns.PublishInput{
		Message:  snsMsg,
		TopicArn: aws.String(generateSnsTopicArn(d.config, message.Topic)),
	})
	return err
}

func (d *snsSqsDriver) isLoggingEnabled() bool {
	return d.parentBus.Logger != nil
}
