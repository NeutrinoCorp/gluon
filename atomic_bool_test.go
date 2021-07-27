package gluon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicBool(t *testing.T) {
	t.Run("Atomic bool", func(t *testing.T) {
		b := atomicBool(0)
		assert.False(t, b.isSet())
		b.setTrue()
		assert.True(t, b.isSet())
	})
}
