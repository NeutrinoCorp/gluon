package gluon

type BusConfiguration struct {
	ConsumerGroup           string
	RemoteSchemaRegistryURI string
	MajorVersion            int
	EnableLogging           bool
	// Driver Custom driver configuration(s)
	Driver interface{}
}
