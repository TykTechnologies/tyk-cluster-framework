package client

import (
	"encoding/json"
	"errors"
)

func Unmarshal(into Payload, data interface{}, enc Encoding) error {
	switch enc {
	case JSON:
		return unmarshalJSON(into, data)
	default:
		return errors.New("Encoding is not supported!")
	}
}

func unmarshalJSON(into Payload, data interface{}) error {
	decErr := json.Unmarshal(data.([]byte), into)
	return decErr
}
