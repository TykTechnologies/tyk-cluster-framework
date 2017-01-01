package tcf

import (
	"errors"
)

type MessageHandler interface{
	HandleRawMessage(interface{}, encoding) (Payload, error)
}

type DefaultMessageHandler struct {}

func (m *DefaultMessageHandler) HandleRawMessage(rawMessage interface{}, enc encoding) (Payload, error) {
	switch rawMessage.(type) {
	case []byte:
		return m.handleByteArrayMessage(rawMessage.([]byte), enc)
	default:
		return nil, errors.New("Raw message type is not supported")
	}
}

func (m *DefaultMessageHandler) handleByteArrayMessage(rawMessage []byte, enc encoding) (Payload, error) {
	thisPayload, pErr := NewPayload(struct{}{})
	if pErr != nil {
		return nil, pErr
	}

	decodeFailure := Unmarshal(thisPayload, rawMessage, enc)
	return thisPayload, decodeFailure
}

