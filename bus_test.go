package gluon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type dummySchema struct {
	Foo string
}

func TestBus_GetSchemaMetadata(t *testing.T) {
	bus := NewBus("local")
	bus.RegisterSchema(dummySchema{}, WithTopic("foo.topic"))
	meta, err := bus.GetSchemaMetadata(dummySchema{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, "foo.topic", meta.Topic)
	}
}
