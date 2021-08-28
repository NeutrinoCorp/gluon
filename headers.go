package gluon

const (
	headerMessageID     = "message_id"
	headerSource        = "source"
	headerSpecVersion   = "spec_version"
	headerType          = "message_type"
	headerContentType   = "content_type"
	headerSchema        = "content_schema"
	headerSubject       = "message_subject"
	headerTime          = "message_time"
	headerCorrelationID = "correlation_id"
	headerCausationID   = "causation_id"
	headerTraceContext  = "trace_context"
	headerConsumerGroup = "consumer_group"
)

func generateHeaders(msg *TransportMessage, sub *Subscriber) map[string]interface{} {
	return map[string]interface{}{
		headerMessageID:     msg.ID,
		headerSource:        msg.Source,
		headerSpecVersion:   msg.SpecVersion,
		headerType:          msg.Type,
		headerContentType:   msg.DataContentType,
		headerSchema:        msg.DataSchema,
		headerSubject:       msg.Subject,
		headerCorrelationID: msg.CorrelationID,
		headerCausationID:   msg.CausationID,
		headerTraceContext:  msg.TraceContext,
		headerTime:          msg.Time,
		headerConsumerGroup: sub.group,
	}
}
