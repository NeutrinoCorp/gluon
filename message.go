package gluon

import "time"

// CloudEventSpecVersion the current version of the CloudEvents specification
const CloudEventSpecVersion = "1.0"

// Message is the basic unit of communication for Gluon-based systems.
// This struct is the one that will be send as body (or headers for some fields) to the actual
// message broker.
//
// It complies with the CNCF's CloudEvents specification to keep consistency between most Event-Driven
// systems mechanisms.
//
// In addition, it complies with Greg Young's EventStore transaction mechanisms to reduce
// observability overhead when debugging transaction workflows (using correlation and causation IDs).
//
// For more information, access to the following link(s): https://cloudevents.io and
// https://blog.arkency.com/correlation-id-and-causation-id-in-evented-systems/
type Message struct {
	ID          string `json:"id"`
	Source      string `json:"source"`
	SpecVersion string `json:"specversion"`
	Type        string `json:"type"`

	CorrelationID   string      `json:"correlation_id,omitempty"`
	CausationID     string      `json:"causation_id,omitempty"`
	Trace           interface{} `json:"trace,omitempty"`
	DataContentType string      `json:"datacontenttype,omitempty"`
	DataSchema      string      `json:"dataschema,omitempty"`
	Subject         string      `json:"subject,omitempty"`
	Time            time.Time   `json:"time,omitempty"`
	Data            interface{} `json:"data,omitempty"`
}
