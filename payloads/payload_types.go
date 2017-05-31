package payloads

import (
	"errors"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/satori/go.uuid"
	"time"
)

type PayloadType string

const (
	PayloadDefaultPayload PayloadType = "PayloadDefaultPayload"
)

// NewPayload should be used to construct a Payload object and have the remainder properly initialised.
// Returns a Payload that can be sent over the wire
func newPayload(msg interface{}, generateID bool) (Payload, error) {
	switch defaultPayloadConfig.payloadType {
	case PayloadDefaultPayload:
		id := ""
		if generateID {
			id = uuid.NewV4().String()
		}
		d := &DefaultPayload{rawMessage: msg, Encoding: encoding.JSON, Time: time.Now().Unix(), MsgID: id}
		d.Encode()
		return d, nil
	default:
		return nil, errors.New("Payload type not supported")
	}
}

func NewPayload(msg interface{}) (Payload, error) {
	return newPayload(msg, true)
}

func NewPayloadNoID(msg interface{}) (Payload, error) {
	return newPayload(msg, false)
}
