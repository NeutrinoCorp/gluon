package arch

type BusConfiguration struct {
	RemoteSchemaRegistryURI string
	MajorVersion            int
	EnableLogging           bool
	// Driver Custom driver configuration(s)
	Driver interface{}
}
