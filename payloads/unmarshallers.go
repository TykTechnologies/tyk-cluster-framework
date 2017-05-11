package payloads

import (
	"encoding/json"
	"errors"
	tykenc "github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

// Unmarshall provides a generic way to unmarshal payloads
func Unmarshal(into Payload, data interface{}, enc tykenc.Encoding) error {
	switch enc {
	case tykenc.JSON:
		return unmarshalJSON(into, data)
	case tykenc.NONE:
		return unmarshalNone(into, data)
	default:
		return errors.New("encoding.Encoding is not supported!")
	}
}

func unmarshalJSON(into Payload, data interface{}) error {
	decErr := json.Unmarshal(data.([]byte), into)
	return decErr
}

func unmarshalNone(into Payload, data interface{}) error {
	return nil
}
