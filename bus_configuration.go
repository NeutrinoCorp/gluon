package gluon

type BusConfiguration struct {
	ConsumerGroup           string
	RemoteSchemaRegistryURI string
	MajorVersion            int
	// Driver Custom driver configuration(s)
	Driver interface{}
}
