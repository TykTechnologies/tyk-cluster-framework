package verifier

import "errors"

type Verifier interface {
	Init(config interface{}) error
	Verify([]byte, string) error
	Sign([]byte) (string, error)
}

var VerificationFailed error = errors.New("Verification failed")

func NewVerifier(name string, config interface{}) (Verifier, error) {
	switch name {
	case "HMAC256":
		h := HMAC256{}
		err := h.Init(config)
		return &h, err
	default:
		return nil, errors.New("Verifier not implemented!")
	}
}
