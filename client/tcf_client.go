package client

import (
	"github.com/TykTechnologies/tyk-cluster-framework/verifier"
	logger "github.com/TykTechnologies/tykcommon-logger"
)

var log = logger.GetLogger()

type Config struct {
	PayloadType                    PayloadType
	MessageHandlerType             MessageHandlerType
	SetEncodingForPayloadsGlobally bool
	Verifier                       verifier.Verifier
	Handlers                       struct {
		Redis struct {
			MaxIdle     int
			MaxActive   int
			IdleTimeout int
		}
	}
}


// Global Client config
var TCFConfig Config = Config{
	PayloadType:                    PayloadDefaultPayload,
	MessageHandlerType:             MessageHandlerDefaultMessageHandler,
	SetEncodingForPayloadsGlobally: true,
	Verifier: verifier.NewHMACVerifier([]byte("c12e2f92-9055-4d96-8c10-91955c27e4f8")),
}

func init() {

}
