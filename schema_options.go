package gluon

type schemaRegistryOptions struct {
	topic     string
	source    string
	schemaURI string
	version   int
}

// SchemaRegistryOption set a specific configuration for internal schema registry.
type SchemaRegistryOption interface {
	apply(registryOptions *schemaRegistryOptions)
}

type schemaTopicOption string

func (o schemaTopicOption) apply(opts *schemaRegistryOptions) {
	opts.topic = string(o)
}

// WithTopic Set a topic name to a message schema.
func WithTopic(s string) SchemaRegistryOption {
	return schemaTopicOption(s)
}

type schemaSourceOption string

func (o schemaSourceOption) apply(opts *schemaRegistryOptions) {
	opts.source = string(o)
}

// WithSource Set a source to a message schema.
func WithSource(s string) SchemaRegistryOption {
	return schemaSourceOption(s)
}

type schemaURIOption string

func (o schemaURIOption) apply(opts *schemaRegistryOptions) {
	opts.schemaURI = string(o)
}

// WithSchemaDefinition Set a schema definition from a file path or URL.
func WithSchemaDefinition(s string) SchemaRegistryOption {
	return schemaURIOption(s)
}

type schemaVersionOption int

func (o schemaVersionOption) apply(opts *schemaRegistryOptions) {
	opts.version = int(o)
}

// WithSchemaVersion Set a major version for a message schema.
func WithSchemaVersion(v int) SchemaRegistryOption {
	return schemaVersionOption(v)
}
