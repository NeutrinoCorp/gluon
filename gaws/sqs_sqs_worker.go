package gaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/neutrinocorp/gluon"
)

type snsSqsSubscriptionWorker struct {
	parentDriver *snsSqsDriver
	rootSub      *gluon.Subscriber
}

func newSnsSqsSubscriptionWorker(parent *snsSqsDriver) *snsSqsSubscriptionWorker {
	return &snsSqsSubscriptionWorker{parentDriver: parent}
}

func (s *snsSqsSubscriptionWorker) start(ctx context.Context, sub *gluon.Subscriber) error {
	s.rootSub = sub
	go func() {
		receiveTimes := 0
		failedPollingCount := 0
	subscriptionLoop:
		for {
			receiveTimes++
			queueUrl := generateSqsQueueUrl(s.parentDriver.config, s.getDefaultConsumerGroup(sub))
			out, err := s.parentDriver.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:                aws.String(queueUrl),
				AttributeNames:          nil,
				MaxNumberOfMessages:     s.parentDriver.config.GetMaxNumberOfMessagesPolled(),
				MessageAttributeNames:   nil,
				ReceiveRequestAttemptId: nil,
				VisibilityTimeout:       s.parentDriver.config.GetVisibilityTimeout(),
				WaitTimeSeconds:         s.parentDriver.config.GetWaitTimeSeconds(),
			})
			s.logError(err)
			if err != nil && failedPollingCount >= s.parentDriver.config.GetMaxBatchPollingRetries() {
				break
			} else if err != nil {
				failedPollingCount++
				continue
			}
			s.fanOutMessagesProcesses(out.Messages...)

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

func (s *snsSqsSubscriptionWorker) logError(err error) {
	if err == nil {
		return
	}

	if s.parentDriver.parentBus.Logger != nil && s.parentDriver.isLoggingEnabled() {
		s.parentDriver.parentBus.Logger.Print(err)
	}
}

func (s *snsSqsSubscriptionWorker) getDefaultConsumerGroup(sub *gluon.Subscriber) string {
	if group := sub.GetGroup(); group != "" {
		return group
	}
	return s.parentDriver.parentBus.Configuration.ConsumerGroup
}

func (s *snsSqsSubscriptionWorker) fanOutMessagesProcesses(msgs ...types.Message) {
	for _, msg := range msgs {
		go s.processMessage(msg)
	}
}

func (s *snsSqsSubscriptionWorker) processMessage(snsMessage types.Message) {
	gluonMsg, err := unmarshalSnsMessage(snsMessage.Body)
	s.logError(err)
	if err != nil {
		return
	}
	go s.execMessageHandler(snsMessage, gluonMsg, s.rootSub)
}

func (s *snsSqsSubscriptionWorker) execMessageHandler(snsMessage types.Message, msg *gluon.TransportMessage,
	sub *gluon.Subscriber) {
	// if procs failed, change message visibility to backoff value
	// if procs succeed, remove message from queue
	scopedCtx := context.Background()
	queueUrl := aws.String(generateSqsQueueUrl(s.parentDriver.config, s.getDefaultConsumerGroup(sub)))
	err := s.parentDriver.messageHandler(scopedCtx, sub, msg)
	s.logError(err)
	if err != nil {
		s.logError(err)
		return
	}

	_, err = s.parentDriver.sqsClient.DeleteMessage(scopedCtx, &sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: snsMessage.ReceiptHandle,
	})
	s.logError(err)
}
