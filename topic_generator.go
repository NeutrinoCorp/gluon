package gluon

import (
	"strconv"
	"strings"
)

// GenerateEventTopic creates a topic from the given event following
// the Async API nomenclature.
func GenerateEventTopic(b *Broker, ev interface{}) string {
	topic := GenerateMessageKey("", false, ev)
	topic = "event." + topic
	return AddTopicPrefix(b, topic)
}

// AddTopicPrefix aggregates Broker configuration to the topic in order
// to comply with Async API topic naming convention.
//
// Nomenclature: org_name.service_name.major_version.message_type.entity.action/fact
//
// 	e.g. neutrino.users.1.event.credentials.added
func AddTopicPrefix(b *Broker, topic string) string {
	buffer := strings.Builder{}
	buffer.WriteString(strings.ToLower(b.Config.Organization) + ".")
	buffer.WriteString(strings.ToLower(b.Config.Service) + ".")
	buffer.WriteString(strconv.Itoa(int(b.Config.MajorVersion)) + ".")
	buffer.WriteString(topic)
	return buffer.String()
}
