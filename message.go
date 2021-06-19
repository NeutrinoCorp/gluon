package gluon

import "time"

// Message is the basic unit of communication for Gluon-based systems.
// This struct is the one that will be send as body (or headers for some fields) to the actual
// message broker.
//
// It complies with the CNCF's CloudEvents specification to keep consistency between most Event-Driven
// systems mechanisms.
//
// For more information, access to the following link: https://cloudevents.io
type Message struct {
	ID          string `json:"id"`
	Source      string `json:"source"`
	SpecVersion string `json:"specversion"`
	Type        string `json:"type"`

	CorrelationID   string      `json:"correlation_id,omitempty"`
	Trace           interface{} `json:"trace,omitempty"`
	DataContentType *string     `json:"datacontenttype,omitempty"`
	DataSchema      *string     `json:"dataschema,omitempty"`
	Subject         *string     `json:"subject,omitempty"`
	Time            *time.Time  `json:"time,omitempty"`
	Data            []byte      `json:"data,omitempty"`
}
