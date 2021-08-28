package gluon

import "time"

type Message struct {
	Headers map[string]interface{}
	Data    interface{}
}

func (m Message) GetMessageID() string {
	return m.Headers[headerMessageID].(string)
}

func (m Message) GetSource() string {
	return m.Headers[headerSource].(string)
}

func (m Message) GetSpecVersion() string {
	return m.Headers[headerSpecVersion].(string)
}

func (m Message) GetMessageType() string {
	return m.Headers[headerType].(string)
}

func (m Message) GetContentType() string {
	return m.Headers[headerContentType].(string)
}

func (m Message) GetSchema() string {
	return m.Headers[headerSchema].(string)
}

func (m Message) GetSubject() string {
	return m.Headers[headerSubject].(string)
}

func (m Message) GetCorrelationID() string {
	return m.Headers[headerCorrelationID].(string)
}

func (m Message) GetCausationID() string {
	return m.Headers[headerCausationID].(string)
}

func (m Message) GetTraceContext() interface{} {
	return m.Headers[headerTraceContext]
}

func (m Message) GetMessageTime() time.Time {
	timeStr := m.Headers[headerTime].(string)
	t, _ := time.Parse(time.RFC3339, timeStr)
	return t
}

func (m Message) GetConsumerGroup() string {
	return m.Headers[headerConsumerGroup].(string)
}
