package gluon

import "time"

type IntegrationEvent struct {
	ID          string `json:"id"`
	Source      string `json:"source"`
	SpecVersion string `json:"specversion"`
	Type        string `json:"type"`
	Retries     int    `json:"retries"`

	DataContentType *string    `json:"datacontenttype"`
	DataSchema      *string    `json:"dataschema"`
	Subject         *string    `json:"subject"`
	Time            *time.Time `json:"time"`
	Data            []byte     `json:"data"`
}
