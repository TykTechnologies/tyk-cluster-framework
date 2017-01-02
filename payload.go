package tcf

import (
	"encoding/json"
	"errors"
	"github.com/TykTechnologies/logrus"
)

// A payload is a type of object that can be used to send around a queue managed by TCF
type Payload interface {
	Verify() error
	Encode() (error)
	DecodeMessage(interface{}) (error)
	SetEncoding(encoding)
}

// DefaultPayload is the default payload that is used by TCF
	type DefaultPayload struct {
	Message interface{}
	Encoding encoding
	Sig     string
	Time    int64
}

// Verify will check the signature if enabled
func (p *DefaultPayload) Verify() error {
	log.WithFields(logrus.Fields{
		"prefix": "tcf.defaultpayload",
	}).Warning("TODO: Implement Verification logic in default paylolad handler!")

	return nil
}

// Encode will convert the payload into the baseline encoding type to send over the wire
func (p *DefaultPayload) Encode() (error) {
	switch p.Encoding {
	case JSON:
		j, err := json.Marshal(p.Message)
		if err != nil {
			return err
		}
		p.Message = string(j)
		return nil
	default:
		return errors.New("Encoding is not supported!")
	}
	return nil
}

func (p *DefaultPayload) getBytes() ([]byte, error) {
	switch p.Message.(type) {
	case []byte:
		return p.Message.([]byte), nil
	case string:
		return []byte(p.Message.(string)), nil
	default:
		return []byte{}, errors.New("Can't convert type to byte array")
	}
}

// DecodeMessage will decode the "message" component of the payload into an object
func (p *DefaultPayload) DecodeMessage(into interface{}) error {
	switch p.Encoding {
	case JSON:
		// We are assuming a type here, not ideal
		toDecode, bErr := p.getBytes()
		if bErr != nil {
			return bErr
		}
		err := json.Unmarshal(toDecode, into)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("Encoding is not supported!")
	}
	return nil
}

func (p *DefaultPayload) SetEncoding(enc encoding) {
	p.Encoding = enc
}