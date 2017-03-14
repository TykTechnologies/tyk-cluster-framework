package client

import (
	"errors"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

type DummyClient struct {
	ClientHandler
	Hostname string
	Port     int
	SubscribeChan chan string
}

func (c *DummyClient) Init(config interface{}) error {
	c.SubscribeChan = make(chan string)
	return nil
}

func (c *DummyClient) Stop() error {
	return nil
}

func (c *DummyClient) Connect() error {
	if c.Hostname == "" || c.Port == 0 {
		return errors.New("Hostname or port are null value!")
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.dummyclient",
	}).Info("Connected to: ", c.Hostname, ":", c.Port)
	return nil
}

func (c *DummyClient) Publish(filter string, p Payload) error {
	payload := make(map[string]interface{})
	decErr := p.DecodeMessage(&payload)
	if decErr != nil {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.dummyclient",
		}).Error("Error: decoding failed: ", decErr)
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.dummyclient",
	}).Info("Publishing: ", payload)
	return nil
}

func (c *DummyClient) notifySub(channel string) {
	select {
	case c.SubscribeChan <- channel:
	default:
	}
}

func (c *DummyClient) Subscribe(filter string, handler PayloadHandler) (chan string, error) {
	log.WithFields(logrus.Fields{
		"prefix": "tcf.dummyclient",
	}).Info("Subscribed: ", filter)

	/*

		A real handler will need to:
			- Call HandleRawMessage() with the data value of the inbound payload to convert it
			- This will pass that value to the actual payload handler in order to generalise the inbound pattern
	*/
	c.notifySub(filter)

	return c.SubscribeChan, nil
}

func (c *DummyClient) SetEncoding(enc encoding.Encoding) error {
	return nil
}

func (c *DummyClient) Broadcast(filter string, payload Payload, interval int) error {
	return nil

}

func (c *DummyClient) StopBroadcast (f string) error {
	return nil
}
