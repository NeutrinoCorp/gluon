package gluon

import (
	"math/rand"
	"strconv"
)

// IDFactory component used to generate unique identifiers
type IDFactory interface {
	// NewID generates a new unique identifier
	NewID() (string, error)
}

// RandomIDFactory default unique identifier factory using math/rand package
type RandomIDFactory struct {
}

var _ IDFactory = &RandomIDFactory{}

func (r RandomIDFactory) NewID() (string, error) {
	return strconv.Itoa(rand.Int()), nil
}
