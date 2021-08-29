package gkafka

// Apache Kafka custom gluon headers
const (
	HeaderOffset    = "kafka-offset"
	HeaderPartition = "kafka-partition"

	// Internal Kafka message headers

	headerMessageID     = "ce_id"
	headerSource        = "ce_source"
	headerSpecVersion   = "ce_specversion"
	headerMessageType   = "ce_type"
	headerMessageTime   = "ce_time"
	headerContentType   = "content_type"
	headerSchema        = "schema"
	headerSubject       = "subject"
	headerCorrelationID = "gl_correlation_id"
	headerCausationID   = "gl_causation_id"
)
