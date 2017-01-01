package tcf


type Config struct {
	PayloadType PayloadType
	MessageHandlerType MessageHandlerType
	SetEncodingForPayloadsGlobally bool
	Handlers struct {
		Redis struct {
			MaxIdle int
			MaxActive int
			IdleTimeout int

		}
	}
}

// Global config
var TCFConfig Config = Config{
	PayloadType:PayloadDefaultPayload,
	MessageHandlerType: MessageHandlerDefaultMessageHandler,
	SetEncodingForPayloadsGlobally: true,
}

func init() {

}