package gluon

type gluonContextKey string

const (
	contextCorrelationID gluonContextKey = "gluon-correlation-id"
	contextMessageID     gluonContextKey = "gluon-message-id"
)
