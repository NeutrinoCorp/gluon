package gluon

// CloudEventsSpecVersion The CloudEvents specification version used by `Gluon` internals.
const CloudEventsSpecVersion = "1.0"

// TransportMessage Is the basic unit of data transportation (also known as integration event).
//
// Based on the CloudEvents specification (a project from the CNCF), the payload of this structure was made to comply
// with most of event-driven systems.
//
// For greater performance gains in further streaming pipelines, it is widely recommended to transport small pieces of
// data (<64 KB). In addition, there are infrastructure vendors that might accept up to a fixed message size.
//
// For more information about the fields that compose this structure, check
// https://github.com/cloudevents/spec/blob/master/spec.md
type TransportMessage struct {
	ID          string `json:"id"`
	Source      string `json:"source"`
	SpecVersion string `json:"specversion"`
	Type        string `json:"type"`
	Data        []byte `json:"data"`

	// Optional fields

	DataContentType string `json:"datacontenttype,omitempty"`
	DataSchema      string `json:"dataschema,omitempty"`
	Subject         string `json:"subject,omitempty"`
	Time            string `json:"time,omitempty"`

	// Custom Gluon fields

	CorrelationID string      `json:"gluon_correlation_id"`
	CausationID   string      `json:"gluon_causation_id"`
	TraceContext  interface{} `json:"gluon_trace_context"`

	// Internal fields
	Topic string `json:"-"`
}
