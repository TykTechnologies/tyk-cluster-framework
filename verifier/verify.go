package verifier

import "errors"

// Verifier provides an interface to enable the cryptographic verification of payload messages
type Verifier interface {
	Init(config interface{}) error
	Verify([]byte, string) error
	Sign([]byte) (string, error)
}

var VerificationFailed error = errors.New("Verification failed")

// NewVerifier will return a verifier for the specified name. Currently only shared-secret HMAC is supported.
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
