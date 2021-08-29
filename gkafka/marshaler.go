package gkafka

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/gluon"
)

type dataEncoder struct {
	data []byte
}

func (d dataEncoder) Encode() ([]byte, error) {
	return d.data, nil
}

func (d dataEncoder) Length() int {
	return len(d.data)
}

var _ sarama.Encoder = dataEncoder{}

func marshalKafkaMessage(msg *gluon.TransportMessage) *sarama.ProducerMessage {
	ts, _ := time.Parse(time.RFC3339, msg.Time)
	return &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Key:       sarama.StringEncoder(msg.ID),
		Value:     dataEncoder{data: msg.Data},
		Headers:   marshalKafkaHeaders(msg),
		Timestamp: ts,
	}
}

func unmarshalKafkaMessage(kMsg *sarama.ConsumerMessage, msg *gluon.TransportMessage) {
	msg.Data = kMsg.Value
	msg.Topic = kMsg.Topic
	unmarshalKafkaHeaders(kMsg, msg)
}

func marshalKafkaHeaders(msg *gluon.TransportMessage) []sarama.RecordHeader {
	return []sarama.RecordHeader{
		{
			Key:   []byte("ce_id"),
			Value: []byte(msg.ID),
		},
		{
			Key:   []byte("ce_source"),
			Value: []byte(msg.Source),
		},
		{
			Key:   []byte("ce_specversion"),
			Value: []byte(msg.SpecVersion),
		},
		{
			Key:   []byte("ce_type"),
			Value: []byte(msg.Type),
		},
		{
			Key:   []byte("ce_time"),
			Value: []byte(msg.Time),
		},
		{
			Key:   []byte("content_type"),
			Value: []byte(msg.DataContentType),
		},
		{
			Key:   []byte("schema"),
			Value: []byte(msg.DataSchema),
		},
		{
			Key:   []byte("subject"),
			Value: []byte(msg.Subject),
		},
		{
			Key:   []byte("gl_correlation_id"),
			Value: []byte(msg.CorrelationID),
		},
		{
			Key:   []byte("gl_causation_id"),
			Value: []byte(msg.CausationID),
		},
	}
}

func unmarshalKafkaHeaders(kMsg *sarama.ConsumerMessage, msg *gluon.TransportMessage) {
	msg.DriverHeaders = map[string]string{}
	msg.DriverHeaders[HeaderOffset] = strconv.Itoa(int(kMsg.Offset))
	msg.DriverHeaders[HeaderPartition] = strconv.Itoa(int(kMsg.Partition))
	for _, v := range kMsg.Headers {
		switch string(v.Key) {
		case headerMessageID:
			msg.ID = string(v.Value)
		case headerSource:
			msg.Source = string(v.Value)
		case headerSpecVersion:
			msg.SpecVersion = string(v.Value)
		case headerMessageType:
			msg.Type = string(v.Value)
		case headerMessageTime:
			msg.Time = string(v.Value)
		case headerContentType:
			msg.DataContentType = string(v.Value)
		case headerSchema:
			msg.DataSchema = string(v.Value)
		case headerSubject:
			msg.Subject = string(v.Value)
		case headerCorrelationID:
			msg.CorrelationID = string(v.Value)
		case headerCausationID:
			msg.CausationID = string(v.Value)
		}
	}
}
