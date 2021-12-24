package gluon

// Marshaler Is a composable component which parses given data into a specific codec in binary format.
//
// The Marshaler composable component is used by `Gluon` internals to preserve a specific codec for Message(s)
// which are transported through stream pipelines.
//
// The default Marshaler is MarshalerJSON.
type Marshaler interface {
	GetContentType() string
	Marshal(schemaDef string, v interface{}) ([]byte, error)
	Unmarshal(schemaDef string, data []byte, v interface{}) error
}

var defaultMarshaler Marshaler = MarshalerJSON{}
