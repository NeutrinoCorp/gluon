package arch

import "github.com/google/uuid"

type FactoryUUID struct{}

var _ IDFactory = FactoryUUID{}

func (f FactoryUUID) NewID() (string, error) {
	return uuid.NewString(), nil
}
