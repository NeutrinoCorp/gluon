package gaws

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

var generateSnsTopicArnTestSuite = []struct {
	config SnsSqsConfig
	topic  string
	exp    string
}{
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{},
			AccountID: "",
		},
		topic: "",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{},
			AccountID: "",
		},
		topic: "ncorp-places-marketplace-dev-1-event-product-published",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{},
			AccountID: "1234567890",
		},
		topic: "ncorp-places-marketplace-dev-1-event-product-published",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{
				Region: "us-west-2",
			},
			AccountID: "1234567890",
		},
		topic: "",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{
				Region: "eu-west-1",
			},
			AccountID: "0987654321",
		},
		topic: "ncorp-wallet-users_manager-prod-1-event-subscription-paid",
		exp:   "arn:aws:sns:eu-west-1:0987654321:ncorp-wallet-users_manager-prod-1-event-subscription-paid",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{
				Region: "us-east-1",
			},
			AccountID: "1234567890",
		},
		topic: "ncorp-places-marketplace-dev-1-event-product-published",
		exp:   "arn:aws:sns:us-east-1:1234567890:ncorp-places-marketplace-dev-1-event-product-published",
	},
}

func TestGenerateSnsTopicArn(t *testing.T) {
	for _, tt := range generateSnsTopicArnTestSuite {
		t.Run("", func(t *testing.T) {
			arn := generateSnsTopicArn(tt.config, tt.topic)
			assert.Equal(t, tt.exp, arn)
		})
	}
}

var generateSqsQueueUrlTestSuite = []struct {
	config SnsSqsConfig
	group  string
	exp    string
}{
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{},
			AccountID: "",
		},
		group: "",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{},
			AccountID: "",
		},
		group: "ncorp-places-marketplace-dev-1",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{},
			AccountID: "1234567890",
		},
		group: "ncorp-places-marketplace-dev-1",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{
				Region: "us-west-2",
			},
			AccountID: "1234567890",
		},
		group: "",
		exp:   "",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{
				Region: "eu-west-1",
			},
			AccountID: "0987654321",
		},
		group: "ncorp-wallet-users_manager-prod-1",
		exp:   "https://sqs.eu-west-1.amazonaws.com/0987654321/ncorp-wallet-users_manager-prod-1",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{
				Region: "us-east-1",
			},
			AccountID: "1234567890",
		},
		group: "ncorp-places-marketplace-dev-1",
		exp:   "https://sqs.us-east-1.amazonaws.com/1234567890/ncorp-places-marketplace-dev-1",
	},
	{
		config: SnsSqsConfig{
			AwsConfig: aws.Config{
				Region: "us-east-1",
			},
			AccountID: "1234567890",
		},
		group: "ncorp.places.marketplace.dev.1",
		exp:   "https://sqs.us-east-1.amazonaws.com/1234567890/ncorp-places-marketplace-dev-1",
	},
}

func TestGenerateSqsQueueUrl(t *testing.T) {
	for _, tt := range generateSqsQueueUrlTestSuite {
		t.Run("", func(t *testing.T) {
			queueUrl := generateSqsQueueUrl(tt.config, tt.group)
			assert.Equal(t, tt.exp, queueUrl)
		})
	}
}
