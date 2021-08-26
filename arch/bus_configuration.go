package arch

type BusConfiguration struct {
	SchemaRegistryURI string
	MajorVersion      int
	EnableLogging     bool
	// Driver Custom driver configuration(s)
	Driver interface{}
}
