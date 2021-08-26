package arch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var messageRegistryTestCases = []struct {
	In   interface{}
	Want MessageMetadata
}{
	{
		In: struct {
			EntityID    string
			TriggeredAt time.Time
		}{
			EntityID:    "1",
			TriggeredAt: time.Now().UTC(),
		},
		Want: MessageMetadata{
			Topic:         "org.neutrino.dummy.triggered",
			Source:        "",
			Subject:       "",
			SchemaURI:     "",
			SchemaVersion: 1,
		},
	},
	{
		In: sensorDummyEvent{
			SensorID: "abc-1-f",
		},
		Want: MessageMetadata{
			Topic:         "org.neutrino.sensor.activated",
			Source:        "",
			Subject:       "",
			SchemaURI:     "",
			SchemaVersion: 10,
		},
	},
	{
		In: "foo",
		Want: MessageMetadata{
			Topic:         "org.neutrino.string.registered",
			Source:        "",
			Subject:       "",
			SchemaURI:     "",
			SchemaVersion: -1,
		},
	},
}

type sensorDummyEvent struct {
	SensorID string
}

func TestTopicRegistry_Register(t *testing.T) {
	registry := newMessageRegistry()
	for _, tt := range messageRegistryTestCases {
		t.Run("", func(t *testing.T) {
			registry.register(tt.Want, tt.In)
			expTopic, _ := registry.get(tt.In)
			assert.EqualValues(t, tt.Want, expTopic)
		})
	}
}
