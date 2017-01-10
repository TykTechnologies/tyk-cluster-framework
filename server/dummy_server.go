package server

import (
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/client"

)

type DummyServer struct {
	broadcast bool
}

func (s *DummyServer) Init(config interface{}) error {
	return nil
}

func (s *DummyServer) Listen() error {
	return nil
}

func (s *DummyServer) Publish(filter string, p client.Payload) error {
	payload := make(map[string]interface{})
	decErr := p.DecodeMessage(&payload)
	if decErr != nil {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.dummyserver",
		}).Error("Error: decoding failed: ", decErr)
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.dummyserver",
	}).Info("Publishing: ", payload)

	return nil
}

func (s *DummyServer) EnableBroadcast(enabled bool) {
	s.broadcast = enabled
}

func (s *DummyServer) SetEncoding(enc encoding) error {
	return nil
}