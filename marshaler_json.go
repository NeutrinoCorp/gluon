package gluon

import (
	json "github.com/json-iterator/go"
)

type MarshalerJSON struct{}

var _ Marshaler = MarshalerJSON{}

func (m MarshalerJSON) GetContentType() string {
	return "application/json"
}

func (m MarshalerJSON) Marshal(_ string, v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (m MarshalerJSON) Unmarshal(_ string, data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
