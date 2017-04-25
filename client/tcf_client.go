package client

import (
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	logger "github.com/TykTechnologies/tykcommon-logger"
)

var log = logger.GetLogger()

// RedisOptions provides extended redis options to manage connectivity
type RedisOptions struct {
	MaxIdle     int
	MaxActive   int
	IdleTimeout int
}

// Config represents the main options to use in the framework
type Config struct {
	PayloadType                    payloads.PayloadType
	MessageHandlerType             MessageHandlerType
	SetEncodingForPayloadsGlobally bool
	Handlers                       struct {
		Redis RedisOptions
	}
}

// Global Client config
var TCFConfig = Config{
	PayloadType:                    payloads.PayloadDefaultPayload,
	MessageHandlerType:             MessageHandlerDefaultMessageHandler,
	SetEncodingForPayloadsGlobally: true,
}

func init() {

}

func SetPayloadType(pType payloads.PayloadType) {
	TCFConfig.PayloadType = pType
}

func SetMessageHandlerType(mhType MessageHandlerType) {
	TCFConfig.MessageHandlerType = mhType
}

func SetEncodingForPayloadsGlobally(t bool) {
	TCFConfig.SetEncodingForPayloadsGlobally = t
}

func SetRedisHandlerOptions(redisOptions RedisOptions) {
	TCFConfig.Handlers.Redis = redisOptions
}
