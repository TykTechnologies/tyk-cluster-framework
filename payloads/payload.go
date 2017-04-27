package payloads

import (
	"encoding/json"
	"errors"
	"fmt"
	tykenc "github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"time"
)

// A payload is a type of object that can be used to send around a queue managed by TCF
type Payload interface {
	Verify() error
	Encode() error
	DecodeMessage(interface{}) error
	SetEncoding(tykenc.Encoding)
	SetTopic(string)
	GetTopic() string
	Copy() Payload
	TimeStamp() time.Time
	From() string
	SetFrom(string)
}

// DefaultPayload is the default payload that is used by TCF
type DefaultPayload struct {
	Message    interface{}
	rawMessage interface{}
	Encoding   tykenc.Encoding
	Sig        string
	Time       int64
	Topic      string
	FromID     string
}

// TimeStamp will set the TS of the payload
func (p *DefaultPayload) TimeStamp() time.Time {
	return time.Unix(p.Time, 0)
}

func (p *DefaultPayload) From() string {
	return p.FromID
}

func (p *DefaultPayload) SetFrom(id string) {
	p.FromID = id
}

func (p *DefaultPayload) SetTopic(topic string) {
	p.Topic = topic
}

func (p *DefaultPayload) GetTopic() string {
	return p.Topic
}

// Verify will check the signature if enabled
func (p *DefaultPayload) Verify() error {
	if p.Message == nil {
		return nil
	}

	switch p.Message.(type) {
	case []byte:
		return defaultPayloadConfig.verifier.Verify(p.Message.([]byte), p.Sig)
	case string:
		return defaultPayloadConfig.verifier.Verify([]byte(p.Message.(string)), p.Sig)
	default:
		return fmt.Errorf("Cannot verify payload because not a byte array or string: %v", p.Message)
	}

	return nil
}

// Encode will convert the payload into the baseline encoding.Encoding type to send over the wire
func (p *DefaultPayload) Encode() error {
	switch p.Encoding {
	case tykenc.JSON:
		j, err := json.Marshal(p.rawMessage)
		if err != nil {
			return err
		}
		p.Message = string(j)

		// Sign
		p.Sig, err = defaultPayloadConfig.verifier.Sign(j)
		return err

	case tykenc.NONE:
		return nil

	default:
		return errors.New("encoding.Encoding is not supported!")
	}

	return nil
}

func (p *DefaultPayload) getBytes() ([]byte, error) {
	switch p.Message.(type) {
	case []byte:
		return p.Message.([]byte), nil
	case string:
		return []byte(p.Message.(string)), nil
	case nil:
		return []byte("{}"), nil
	default:
		return []byte{}, errors.New("Can't convert type to byte array")
	}
}

// DecodeMessage will decode the "message" component of the payload into an object
func (p *DefaultPayload) DecodeMessage(into interface{}) error {
	switch p.Encoding {
	case tykenc.JSON:
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
	case tykenc.NONE:
		return nil
	default:
		return errors.New("encoding.Encoding is not supported!")
	}
	return nil
}

// SetEncoding will set the encoding of the payloads
func (p *DefaultPayload) SetEncoding(enc tykenc.Encoding) {
	p.Encoding = enc
}

// Copy will create a copy of the object
func (p *DefaultPayload) Copy() Payload {
	np := &DefaultPayload{
		Message:    p.Message,
		rawMessage: p.rawMessage,
		Encoding:   p.Encoding,
		Sig:        p.Sig,
		Time:       p.Time,
		Topic:      p.Topic,
		FromID:     p.From(),
	}

	return np
}
