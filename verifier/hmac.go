package verifier

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
)

type HMAC256 struct{
	secret []byte
}

func (h *HMAC256) Init(config interface{}) error {
	switch config.(type) {
	case []byte:
		h.secret = config.([]byte)
	case string:
		h.secret = []byte(config.(string))
	default:
		return errors.New("HMAC secret must be string or byte array")
	}

	return nil
}

func (h *HMAC256) Verify(message []byte, signature string) error {
	computedSig, _ := h.Sign(message)
	if computedSig != signature {
		//fmt.Println(computedSig)
		//fmt.Println(signature)
		return VerificationFailed
	}

	return nil
}

func (h *HMAC256) Sign(message []byte) (string, error) {
	hm := hmac.New(sha256.New, h.secret)
	hm.Write(message)
	sig := base64.StdEncoding.EncodeToString(hm.Sum(nil))

	return sig, nil

}

func NewHMACVerifier(secret []byte) *HMAC256 {
	hm := &HMAC256{}
	hm.Init(secret)

	return hm
}