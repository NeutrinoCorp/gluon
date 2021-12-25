package gaws

import (
	"strings"
)

func generateSnsTopicArn(cfg SnsSqsConfig, topic string) string {
	if cfg.AwsConfig.Region == "" || cfg.AccountID == "" || topic == "" {
		return ""
	}

	builder := strings.Builder{}
	builder.WriteString("arn:aws:sns:")
	builder.WriteString(cfg.AwsConfig.Region)
	builder.WriteString(":")
	builder.WriteString(cfg.AccountID)
	builder.WriteString(":")
	// Note: Gluon constructs topics using '.' character. AWS SNS does not accept this character
	// For more information: https://docs.aws.amazon.com/sns/latest/dg/sns-create-topic.html
	builder.WriteString(strings.ReplaceAll(topic, ".", "-"))
	return builder.String()
}

func generateSqsQueueUrl(cfg SnsSqsConfig, group string) string {
	if cfg.CustomSqsEndpoint != "" {
		return cfg.CustomSqsEndpoint + "/" + cfg.AccountID + "/" + strings.ReplaceAll(group, ".", "-")
	} else if cfg.AwsConfig.Region == "" || cfg.AccountID == "" || group == "" {
		return ""
	}

	builder := strings.Builder{}
	builder.WriteString("https://sqs.")
	builder.WriteString(cfg.AwsConfig.Region)
	builder.WriteString(".amazonaws.com/")
	builder.WriteString(cfg.AccountID)
	builder.WriteString("/")
	builder.WriteString(strings.ReplaceAll(group, ".", "-"))
	return builder.String()
}
