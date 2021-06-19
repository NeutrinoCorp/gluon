package gmemory

import "github.com/neutrinocorp/gluon"

func init() {
	gluon.Register(&Driver{})
}

type Driver struct{}

var _ gluon.Driver = &Driver{}

func (d Driver) NewWorker(b *gluon.Broker) gluon.Worker {
	return newWorker(b)
}
