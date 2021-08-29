package gutil

import "strings"

// TopicGeneratorArgs GenerateTopicName arguments.
type TopicGeneratorArgs struct {
	IsRetry bool
	IsDLQ   bool

	ReversedDomain string
	Service        string
	MessageType    string
	Entity         string
	Action         string
}

const (
	// MessageTypeCommand Is a concrete action that is requested to be processed.
	MessageTypeCommand = "command"
	// MessageTypeEvent Is a fact that happened within the system.
	MessageTypeEvent = "event"
)

// GenerateTopicName Construct a topic name using the Async API specification.
//
// Generated topic names may be used along with wildcards to filter messages (only if infrastructure has that feature,
// like RabbitMQ).
//
// (e.g. Filter all user commands: org.neutrinocorp.iam.command.user.*)
func GenerateTopicName(args TopicGeneratorArgs) string {
	builder := strings.Builder{}
	builder.WriteString(args.ReversedDomain + ".")
	builder.WriteString(args.Service + ".")
	builder.WriteString(args.MessageType + ".")
	builder.WriteString(args.Entity + ".")
	builder.WriteString(args.Action)

	if args.IsRetry {
		builder.WriteString(".retry")
	} else if args.IsDLQ {
		builder.WriteString(".dlq")
	}

	return builder.String()
}
