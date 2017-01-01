package tcf

import (
	"fmt"
	"errors"
)

type DummyClient struct{
	ClientHandler
	Hostname string
	Port int
}

func (c *DummyClient) Init(config interface{}) error {
	return nil
}

func (c *DummyClient) Connect() error {
	if c.Hostname == "" || c.Port == 0 {
		return errors.New("Hostname or port are null value!")
	}
	fmt.Printf("CONNECTED: %v:%v \n", c.Hostname, c.Port)
	return nil
}

func (c *DummyClient) Publish(filter string, p Payload) error {
	payload := make(map[string]interface{})
	decErr := p.DecodeMessage(&payload)
	if decErr != nil {
		fmt.Printf("Error: decoding failed: %v\n", decErr)
	}

	fmt.Printf("PUBLISHING: %v \n", payload)
	return nil
}

func (c *DummyClient) Subscribe(filter string, handler PayloadHandler) error {
	fmt.Printf("SUBSCRIBED: %v \n", filter)

	/*
	thisMessage := Notification{}
	err := json.Unmarshal(message.Data, &thisMessage)

	A real handler will need to:
		- Call HandleRawMessage() with the data value of the inbound payload to convert it
		- This will pass that value to the actual payload handler in order to generalise the inbound pattern
	 */
	return nil
}

func (c *DummyClient) SetEncoding(enc encoding) error {
	return nil
}