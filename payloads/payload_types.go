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

// NewPayload should be used to construct a Payload object and have the remainder properly initialised.
// Returns a Payload that can be sent over the wire
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
