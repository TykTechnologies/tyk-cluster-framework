package client

type MessageHandlerType string

const (
	MessageHandlerDefaultMessageHandler MessageHandlerType = "MessageHandlerDefaultMessageHandler"
)

// NewMessagHandler will define the message handler to return based on the MessageHandlerType enums
func NewMessageHandler() MessageHandler {
	switch TCFConfig.MessageHandlerType {
	case MessageHandlerDefaultMessageHandler:
		return &DefaultMessageHandler{}
	default:
		return &DefaultMessageHandler{}

	}
}
