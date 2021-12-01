package gaws

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const (
	defaultVisibilityTimeout         = 10
	defaultWaitTimeSeconds           = 5
	defaultMaxNumberOfMessagesPolled = 10
	defaultMaxBatchPollingRetries    = 0
	defaultFailedPollingBackoff      = time.Second * 5
)

type SnsSqsConfig struct {
	AwsConfig                 aws.Config
	AccountID                 string
	MaxNumberOfMessagesPolled int32
	VisibilityTimeout         int32
	WaitTimeSeconds           int32
	MaxBatchPollingRetries    int
	FailedPollingBackoff      time.Duration
}

func (c SnsSqsConfig) GetMaxNumberOfMessagesPolled() int32 {
	if c.MaxNumberOfMessagesPolled == 0 {
		return defaultMaxNumberOfMessagesPolled
	}
	return c.MaxNumberOfMessagesPolled
}

func (c SnsSqsConfig) GetVisibilityTimeout() int32 {
	if c.VisibilityTimeout == 0 {
		return defaultVisibilityTimeout
	}
	return c.VisibilityTimeout
}

func (c SnsSqsConfig) GetWaitTimeSeconds() int32 {
	if c.WaitTimeSeconds == 0 {
		return defaultWaitTimeSeconds
	}
	return c.WaitTimeSeconds
}

func (c SnsSqsConfig) GetFailedPollingBackoff() time.Duration {
	if c.FailedPollingBackoff == 0 {
		return defaultFailedPollingBackoff
	}
	return c.FailedPollingBackoff
}

func (c SnsSqsConfig) GetMaxBatchPollingRetries() int {
	if c.MaxBatchPollingRetries <= 0 {
		return defaultMaxBatchPollingRetries
	}
	return c.MaxBatchPollingRetries
}
