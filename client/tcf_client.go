package client

import (
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	logger "github.com/TykTechnologies/tykcommon-logger"
)

var log = logger.GetLogger()

type RedisOptions struct {
	MaxIdle     int
	MaxActive   int
	IdleTimeout int
}

type Config struct {
	PayloadType                    payloads.PayloadType
	MessageHandlerType             MessageHandlerType
	SetEncodingForPayloadsGlobally bool
	Handlers                       struct {
		Redis RedisOptions
	}
}

// Global Client config
var TCFConfig Config = Config{
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
