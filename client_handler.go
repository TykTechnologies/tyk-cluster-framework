package tcf

import (
	"fmt"
	"errors"
)

// ClientHandler provides helper functions and wrappers to decode a raw message into a payload object
// to pass onto a payload handler
type ClientHandler struct {}

// HandleRawMessage will take the raw data, payload handler and encoding, decode the value,
// pass it to the handler and return an error if there was a problem
func (c ClientHandler) HandleRawMessage(rawMessage interface{}, payloadHandler PayloadHandler, enc encoding) error {
	// First, decode the message based on it's type and encoding
	msgHandler := NewMessageHandler()
	asPayload, err := msgHandler.HandleRawMessage(rawMessage, enc)
	if err != nil {
		return err
	}

	if asPayload.Verify() != nil {
		return errors.New("Payload verification failed")
	}

	// Call the registered handler
	payloadHandler(asPayload)
	return nil
}

// WrappedHandler returns a payload handler that includes the pre-processing step
func (c ClientHandler) WrappedHandler(rawMessage interface{}, payloadHandler PayloadHandler, enc encoding) PayloadHandler {

	return func(payload Payload) {
		// First, decode the message based on it's type and encoding
		msgHandler := NewMessageHandler()
		asPayload, err := msgHandler.HandleRawMessage(rawMessage, enc)
		if err != nil {
			fmt.Printf("Payload decoding error: %v\n", err)
		}
		payloadHandler(asPayload)
	}
}
