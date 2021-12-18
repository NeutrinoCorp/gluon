package gluon

type BusConfiguration struct {
	ConsumerGroup string
	MajorVersion  int
	// Driver Custom driver configuration(s)
	Driver interface{}
}
