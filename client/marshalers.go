package client

import (
	"encoding/json"
	tykEnc "github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"errors"
)

func Marshal(from Payload, enc tykEnc.Encoding) (interface{}, error) {
	switch enc {
	case tykEnc.JSON:
		return marshalJSON(from)
	default:
		return nil, errors.New("encoding.Encoding is not supported!")
	}
}

func marshalJSON(from Payload) (interface{}, error) {
	// Copy the object, we don;t want to operate on the same payload (NOT IDEAL)
	newPayload := from.Copy()
	// First encode the inner data payload
	newPayload.Encode()
	return json.Marshal(newPayload)
}
