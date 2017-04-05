package payloads

import "github.com/TykTechnologies/tyk-cluster-framework/verifier"

type config struct {
	verifier    verifier.Verifier
	payloadType PayloadType
}

var defaultPayloadConfig config = config{}

func init() {
	defaultPayloadConfig.verifier = verifier.NewHMACVerifier([]byte("c12e2f92-9055-4d96-8c10-91955c27e4f8"))
	defaultPayloadConfig.payloadType = PayloadDefaultPayload
}

// St the verifier to use for payloads
func SetVerifier(v verifier.Verifier) {
	defaultPayloadConfig.verifier = v
}

// Set the payload type to use when encoding and decoding payloads
func SetPayloadType(p PayloadType) {
	defaultPayloadConfig.payloadType = p
}
