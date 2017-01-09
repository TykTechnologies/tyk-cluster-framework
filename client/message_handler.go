package client

import (
	"errors"
)

type MessageHandler interface{
	HandleRawMessage(interface{}, Encoding) (Payload, error)
}

type DefaultMessageHandler struct {}

func (m *DefaultMessageHandler) HandleRawMessage(rawMessage interface{}, enc Encoding) (Payload, error) {
	switch rawMessage.(type) {
	case []byte:
		return m.handleByteArrayMessage(rawMessage.([]byte), enc)
	default:
		return nil, errors.New("Raw message type is not supported")
	}
}

func (m *DefaultMessageHandler) handleByteArrayMessage(rawMessage []byte, enc Encoding) (Payload, error) {
	thisPayload, pErr := NewPayload(struct{}{})
	if pErr != nil {
		return nil, pErr
	}

	decodeFailure := Unmarshal(thisPayload, rawMessage, enc)
	return thisPayload, decodeFailure
}

