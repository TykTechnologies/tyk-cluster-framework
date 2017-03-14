package client

import (
	"errors"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

type MessageHandler interface {
	HandleRawMessage(interface{}, encoding.Encoding) (Payload, error)
}

type DefaultMessageHandler struct{}

func (m *DefaultMessageHandler) HandleRawMessage(rawMessage interface{}, enc encoding.Encoding) (Payload, error) {
	switch rawMessage.(type) {
	case []byte:
		return m.handleByteArrayMessage(rawMessage.([]byte), enc)
	default:
		return nil, errors.New("Raw message type is not supported")
	}
}

func (m *DefaultMessageHandler) handleByteArrayMessage(rawMessage []byte, enc encoding.Encoding) (Payload, error) {
	thisPayload, pErr := NewPayload(struct{}{})
	if pErr != nil {
		return nil, pErr
	}

	decodeFailure := Unmarshal(thisPayload, rawMessage, enc)
	return thisPayload, decodeFailure
}
