package tcf

type MessageHandlerType string

const (
	MessageHandlerDefaultMessageHandler MessageHandlerType = "MessageHandlerDefaultMessageHandler"
)

func NewMessageHandler() MessageHandler {
	switch TCFConfig.MessageHandlerType {
	case MessageHandlerDefaultMessageHandler:
		return &DefaultMessageHandler{}
	default:
		return &DefaultMessageHandler{}

	}
}