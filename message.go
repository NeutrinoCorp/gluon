package gluon

import "time"

// Message is the basic unit of communication for Gluon-based systems.
// This struct is the one that will be send as body (or headers for some fields) to the actual
// message broker.
//
// It has the CNCF's CloudEvent v1 specification body structure to comply with  most Event-Driven
// systems.
type Message struct {
	ID            string      `json:"id"`
	Source        string      `json:"source"`
	SpecVersion   string      `json:"specversion"`
	Type          string      `json:"type"`
	CorrelationID string      `json:"correlation_id"`
	Trace         interface{} `json:"trace"`

	DataContentType *string    `json:"datacontenttype"`
	DataSchema      *string    `json:"dataschema"`
	Subject         *string    `json:"subject"`
	Time            *time.Time `json:"time"`
	Data            []byte     `json:"data"`
}
