package gaws

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/neutrinocorp/gluon"
)

var ErrCannotUnmarshalSnsMessage = errors.New("gluon: Cannot unmarshal AWS SNS message")

type snsMessage struct {
	Message string `json:"Message"`
}

func unmarshalSnsMessage(msg *string) (*gluon.TransportMessage, error) {
	if msg == nil {
		return nil, ErrCannotUnmarshalSnsMessage
	}
	snsMsg := snsMessage{}
	if err := json.Unmarshal([]byte(*msg), &snsMsg); err != nil {
		return nil, err
	}

	gluonMsg := gluon.TransportMessage{}
	if err := json.Unmarshal([]byte(snsMsg.Message), &gluonMsg); err != nil {
		return nil, err
	}
	return &gluonMsg, nil
}

func marshalSnsMessage(msg *gluon.TransportMessage) (*string, error) {
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return aws.String(string(msgJSON)), nil
}
