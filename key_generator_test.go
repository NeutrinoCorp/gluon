package gluon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type startPaymentProcessCommand struct {
	ProcessID string
	PaymentID string
	StartedBy string
	StartTime string
}

type paymentProcessStarted struct {
	PaymentID string
}

type getPaymentProcessQuery struct {
	ProcessID string
}

func TestGenerateKey(t *testing.T) {
	cmd := startPaymentProcessCommand{
		ProcessID: "",
		PaymentID: "",
		StartedBy: "",
		StartTime: "",
	}
	key := GenerateMessageKey("Command", true, cmd)
	assert.Equal(t, "payment_process.start", key)

	event := paymentProcessStarted{PaymentID: ""}
	key = GenerateMessageKey("", false, event)
	assert.Equal(t, "payment_process.started", key)

	query := getPaymentProcessQuery{
		ProcessID: "",
	}
	key = GenerateMessageKey("Query", true, query)
	assert.Equal(t, "payment_process.get", key)
}

func BenchmarkGenerateKey(b *testing.B) {
	cmd := getPaymentProcessQuery{
		ProcessID: "",
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = GenerateMessageKey("Query", true, cmd)
	}
}

func BenchmarkSeparateKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		str := "FooBarBaz"
		separateCamelCase(str)
	}
}
