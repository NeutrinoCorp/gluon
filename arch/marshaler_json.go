package arch

import "encoding/json"

type MarshalerJSON struct{}

var _ Marshaler = MarshalerJSON{}

func (m MarshalerJSON) GetContentType() string {
	return "application/json"
}

func (m MarshalerJSON) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (m MarshalerJSON) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
