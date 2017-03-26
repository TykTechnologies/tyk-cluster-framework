package payloads

import (
	"errors"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"time"
)

type PayloadType string

const (
	PayloadDefaultPayload PayloadType = "PayloadDefaultPayload"
)

func NewPayload(msg interface{}) (Payload, error) {
	switch defaultPayloadConfig.payloadType {
	case PayloadDefaultPayload:
		d := &DefaultPayload{rawMessage: msg, Encoding: encoding.JSON, Time: time.Now().Unix()}
		d.Encode()
		return d, nil
	default:
		return nil, errors.New("Payload type not supported")
	}
}
