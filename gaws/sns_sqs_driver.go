package gaws

import (
	"context"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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

	subMu       sync.Mutex
	subscribers map[string][]*gluon.Subscriber
}

var (
	_                     gluon.Driver = &snsSqsDriver{}
	defaultDriver         *snsSqsDriver
	snsSqsDriverSingleton = sync.Once{}
)

func init() {
	snsSqsDriverSingleton.Do(func() {
		defaultDriver = &snsSqsDriver{
			subMu:       sync.Mutex{},
			subscribers: map[string][]*gluon.Subscriber{},
		}
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
	go func() {
		receiveTimes := 0
	subscriptionLoop:
		for {
			receiveTimes++
			log.Printf("fetching data for %d time", receiveTimes)
			queueUrl := generateSqsQueueUrl(d.config, d.parentBus.Configuration.ConsumerGroup)
			out, err := d.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:                aws.String(queueUrl),
				AttributeNames:          nil,
				MaxNumberOfMessages:     d.config.GetMaxNumberOfMessagesPolled(),
				MessageAttributeNames:   nil,
				ReceiveRequestAttemptId: nil,
				VisibilityTimeout:       0,
				WaitTimeSeconds:         d.config.GetWaitTimeSeconds(),
			})
			d.logError(err)
			if err != nil {
				continue
			}
			d.fanOutMessagesProcesses(out.Messages...)

			select {
			case <-ctx.Done():
				break subscriptionLoop
			default:
				continue
			}
		}
	}()
	return nil
}

func (d *snsSqsDriver) logError(err error) {
	if err == nil {
		return
	}

	log.Print(err)
	if d.parentBus.Logger != nil && d.isLoggingEnabled() {
		d.parentBus.Logger.Print(err)
	}
}

func (d *snsSqsDriver) fanOutMessagesProcesses(msgs ...types.Message) {
	for _, msg := range msgs {
		go d.processMessage(msg)
	}
}

func (d *snsSqsDriver) processMessage(snsMessage types.Message) {
	gluonMsg, err := unmarshalSnsMessage(snsMessage.Body)
	d.logError(err)
	if err != nil {
		return
	}
	for _, sub := range d.subscribers[gluonMsg.Type] {
		go d.execMessageHandler(snsMessage, gluonMsg, sub)
	}
}

func (d *snsSqsDriver) execMessageHandler(snsMessage types.Message, msg *gluon.TransportMessage,
	sub *gluon.Subscriber) {
	// if procs failed, change message visibility to backoff value
	// if procs succeed, remove message from queue
	scopedCtx := context.Background()
	queueUrl := aws.String(generateSqsQueueUrl(d.config, d.getDefaultConsumerGroup(sub)))
	err := d.messageHandler(scopedCtx, sub, msg)
	d.logError(err)
	if err != nil {
		_, err = d.sqsClient.ChangeMessageVisibility(scopedCtx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          queueUrl,
			ReceiptHandle:     snsMessage.ReceiptHandle,
			VisibilityTimeout: d.config.GetFailedProcessBackoff(),
		})
		d.logError(err)
		return
	}

	_, err = d.sqsClient.DeleteMessage(scopedCtx, &sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: snsMessage.ReceiptHandle,
	})
	d.logError(err)
}

func (d *snsSqsDriver) getDefaultConsumerGroup(sub *gluon.Subscriber) string {
	if group := sub.GetGroup(); group != "" {
		return group
	}
	return d.parentBus.Configuration.ConsumerGroup
}

func (d *snsSqsDriver) Shutdown(_ context.Context) error {
	return nil
}

func (d *snsSqsDriver) Subscribe(_ context.Context, subscriber *gluon.Subscriber) error {
	d.subMu.Lock()
	defer d.subMu.Unlock()
	subs, ok := d.subscribers[subscriber.GetTopic()]
	if !ok {
		subs = make([]*gluon.Subscriber, 0)
	}
	subs = append(subs, subscriber)
	d.subscribers[subscriber.GetTopic()] = subs
	return nil
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
	return d.parentBus.Configuration.EnableLogging && d.parentBus.Logger != nil
}
