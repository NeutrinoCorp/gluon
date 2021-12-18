package gluon

type internalSchemaRegistryOptions struct {
	topic      string
	source     string
	schemaName string
	version    int
}

// SchemaRegistryOption set a specific configuration for internal schema registry.
type SchemaRegistryOption interface {
	apply(registryOptions *internalSchemaRegistryOptions)
}

type schemaTopicOption string

func (o schemaTopicOption) apply(opts *internalSchemaRegistryOptions) {
	opts.topic = string(o)
}

// WithTopic Set a topic name to a message schema.
func WithTopic(s string) SchemaRegistryOption {
	return schemaTopicOption(s)
}

type schemaSourceOption string

func (o schemaSourceOption) apply(opts *internalSchemaRegistryOptions) {
	opts.source = string(o)
}

// WithSource Set a source to a message schema.
func WithSource(s string) SchemaRegistryOption {
	return schemaSourceOption(s)
}

type schemaNameOption string

func (o schemaNameOption) apply(opts *internalSchemaRegistryOptions) {
	opts.schemaName = string(o)
}

// WithSchemaName Set the name of the schema stored on the SchemaRegistry.
func WithSchemaName(s string) SchemaRegistryOption {
	return schemaNameOption(s)
}

type schemaVersionOption int

func (o schemaVersionOption) apply(opts *internalSchemaRegistryOptions) {
	opts.version = int(o)
}

// WithSchemaVersion Set a major version for a message schema.
func WithSchemaVersion(v int) SchemaRegistryOption {
	return schemaVersionOption(v)
}
