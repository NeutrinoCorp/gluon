package gutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var topicGeneratorTestCases = []struct {
	In   TopicGeneratorArgs
	Want string
}{
	{
		In: TopicGeneratorArgs{
			IsRetry:        false,
			IsDLQ:          false,
			ReversedDomain: "org.neutrino",
			Service:        "wallet",
			MessageType:    MessageTypeCommand,
			Entity:         "payment",
			Action:         "request",
		},
		Want: "org.neutrino.wallet.command.payment.request",
	},
	{
		In: TopicGeneratorArgs{
			IsRetry:        true,
			IsDLQ:          false,
			ReversedDomain: "org.neutrino",
			Service:        "iam",
			MessageType:    MessageTypeEvent,
			Entity:         "user",
			Action:         "signed_up",
		},
		Want: "org.neutrino.iam.event.user.signed_up.retry",
	},
	{
		In: TopicGeneratorArgs{
			IsRetry:        true,
			IsDLQ:          true,
			ReversedDomain: "org.neutrino",
			Service:        "iam",
			MessageType:    MessageTypeEvent,
			Entity:         "user",
			Action:         "signed_up",
		},
		Want: "org.neutrino.iam.event.user.signed_up.retry",
	},
	{
		In: TopicGeneratorArgs{
			IsRetry:        false,
			IsDLQ:          true,
			ReversedDomain: "org.neutrino",
			Service:        "iam",
			MessageType:    MessageTypeEvent,
			Entity:         "user",
			Action:         "signed_up",
		},
		Want: "org.neutrino.iam.event.user.signed_up.dlq",
	},
}

func TestGenerateTopicName(t *testing.T) {
	for _, tt := range topicGeneratorTestCases {
		t.Run("", func(t *testing.T) {
			topic := GenerateTopicName(tt.In)
			assert.Equal(t, tt.Want, topic)
		})
	}
}
