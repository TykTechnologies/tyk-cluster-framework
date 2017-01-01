package tcf

import (
	"errors"
	"encoding/json"
)

func Unmarshal(into Payload, data interface{}, enc encoding) error {
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
