package payloads

import (
	"encoding/json"
	"errors"
	tykEnc "github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

// Marshal will call the correct marshallers for the payload, because payloads are double-encoded
// (the payload format wraps the internal message payload, which is also encoded)
// Currently JSON is the only supported marshaller
func Marshal(from Payload, enc tykEnc.Encoding) (interface{}, error) {
	switch enc {
	case tykEnc.JSON:
		return marshalJSON(from)
	default:
		return nil, errors.New("encoding.Encoding is not supported!")
	}
}

// marshalJSON will
func marshalJSON(from Payload) (interface{}, error) {
	// Copy the object, we don;t want to operate on the same payload (NOT IDEAL)
	newPayload := from.Copy()
	// First encode the inner data payload
	newPayload.Encode()
	return json.Marshal(newPayload)
}
