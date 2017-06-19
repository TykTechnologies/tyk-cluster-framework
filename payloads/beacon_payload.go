package payloads

import (
	"encoding/json"
	"errors"
	tykenc "github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

// DefaultPayload is the default payload that is used by TCF
type MicroPayload struct {
	M    interface{} `json:"M,omitempty"`
	rawMessage interface{}
	E   tykenc.Encoding `json:"E,omitempty"`
	S        string `json:"S,omitempty"`
	T       int64 `json:"T,omitempty"`
	TP      string `json:"TP,omitempty"`
	F     string `json:"F,omitempty"`
	MI      string `json:"MI,omitempty"`
}

// TimeStamp will set the TS of the payload
func (p *MicroPayload) TimeStamp() time.Time {
	return time.Unix(p.T, 0)
}

func (p *MicroPayload) SetData(data interface{}) {
	p.rawMessage = data
	p.Encode()
}

func (p *MicroPayload) From() string {
	return p.F
}

func (p *MicroPayload) SetFrom(id string) {
	p.F = id
}

func (p *MicroPayload) SetTopic(topic string) {
	p.TP = topic
}

func (p *MicroPayload) GetTopic() string {
	return p.TP
}

func (p *MicroPayload) GetID() string {
	return p.MI
}

// Not implemented in MP
func (p *MicroPayload) Verify() error {

	return nil
}

// Encode will convert the payload into the baseline encoding.Encoding type to send over the wire
func (p *MicroPayload) Encode() error {
	switch p.E {
	case tykenc.JSON:
		j, err := json.Marshal(p.rawMessage)
		if err != nil {
			return err
		}
		p.M = string(j)

		// Sign
		p.S, err = defaultPayloadConfig.verifier.Sign(j)
		return err

	case tykenc.MPK:
		j, err := msgpack.Marshal(p.rawMessage)
		if err != nil {
			return err
		}
		p.M = string(j)

		// Sign
		p.S, err = defaultPayloadConfig.verifier.Sign(j)
		return err

	case tykenc.NONE:
		return nil

	default:
		return errors.New("encoding.Encoding is not supported!")
	}

	return nil
}

func (p *MicroPayload) getBytes() ([]byte, error) {
	switch p.M.(type) {
	case []byte:
		return p.M.([]byte), nil
	case string:
		return []byte(p.M.(string)), nil
	case nil:
		return []byte("{}"), nil
	default:
		return []byte{}, errors.New("Can't convert type to byte array")
	}
}

// DecodeMessage will decode the "message" component of the payload into an object
func (p *MicroPayload) DecodeMessage(into interface{}) error {
	switch p.E {
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

	case tykenc.MPK:
		// We are assuming a type here, not ideal
		toDecode, bErr := p.getBytes()
		if bErr != nil {
			return bErr
		}
		err := msgpack.Unmarshal(toDecode, into)
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
func (p *MicroPayload) SetEncoding(enc tykenc.Encoding) {
	p.E = enc
}

// Copy will create a copy of the object
func (p *MicroPayload) Copy() Payload {
	np := &MicroPayload{
		M:    p.M,
		rawMessage: p.rawMessage,
		E:   p.E,
		S:        p.S,
		T:       p.T,
		TP:      p.TP,
		F:     p.From(),
		MI:      p.MI,
	}

	return np
}
