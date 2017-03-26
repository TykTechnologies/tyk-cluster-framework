package payloads

import (
	"encoding/json"
	"errors"
	tykenc "github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

func Unmarshal(into Payload, data interface{}, enc tykenc.Encoding) error {
	switch enc {
	case tykenc.JSON:
		return unmarshalJSON(into, data)
	default:
		return errors.New("encoding.Encoding is not supported!")
	}
}

func unmarshalJSON(into Payload, data interface{}) error {
	decErr := json.Unmarshal(data.([]byte), into)
	return decErr
}
