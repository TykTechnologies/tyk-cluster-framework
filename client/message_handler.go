package client

import (
	"errors"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
)

type MessageHandler interface {
	HandleRawMessage(interface{}, encoding.Encoding) (payloads.Payload, error)
}

type DefaultMessageHandler struct{}

func (m *DefaultMessageHandler) HandleRawMessage(rawMessage interface{}, enc encoding.Encoding) (payloads.Payload, error) {
	switch rawMessage.(type) {
	case []byte:
		return m.handleByteArrayMessage(rawMessage.([]byte), enc)
	default:
		return nil, errors.New("Raw message type is not supported")
	}
}

func (m *DefaultMessageHandler) handleByteArrayMessage(rawMessage []byte, enc encoding.Encoding) (payloads.Payload, error) {
	thisPayload, pErr := payloads.NewPayload(struct{}{})
	if pErr != nil {
		return nil, pErr
	}

	decodeFailure := payloads.Unmarshal(thisPayload, rawMessage, enc)
	return thisPayload, decodeFailure
}
