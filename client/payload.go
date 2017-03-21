package client

import (
	"encoding/json"
	"errors"
	tykenc "github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"time"
	"fmt"
)

// A payload is a type of object that can be used to send around a queue managed by TCF
type Payload interface {
	Verify() error
	Encode() error
	DecodeMessage(interface{}) error
	SetEncoding(tykenc.Encoding)
	Copy() Payload
	TimeStamp() time.Time
}

// DefaultPayload is the default payload that is used by TCF
type DefaultPayload struct {
	Message interface{}
	rawMessage  interface{}
	RawMessage interface{}
	Encoding tykenc.Encoding
	Sig      string
	Time     int64
}

func (p *DefaultPayload) TimeStamp() time.Time {
	return time.Unix(p.Time, 0)
}

// Verify will check the signature if enabled
func (p *DefaultPayload) Verify() error {
	if p.Message == nil {
		return errors.New("No message to verify, Message is nil")
	}

	switch p.Message.(type) {
	case []byte:
		return TCFConfig.Verifier.Verify(p.Message.([]byte), p.Sig)
	default:
		return fmt.Errorf("Cannot verify payload because not a byte array: %v", p.Message)
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
		p.Message = j

		// Sign
		p.Sig, err = TCFConfig.Verifier.Sign(j)
		return err

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
	default:
		return errors.New("encoding.Encoding is not supported!")
	}
	return nil
}

func (p *DefaultPayload) SetEncoding(enc tykenc.Encoding) {
	p.Encoding = enc
}

func (p *DefaultPayload) Copy() Payload {
	np := &DefaultPayload{
		Message:  p.Message,
		rawMessage: p.rawMessage,
		Encoding: p.Encoding,
		Sig:      p.Sig,
		Time:     p.Time,
	}

	return np
}
