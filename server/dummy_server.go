package server

import (
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
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

func (s *DummyServer) EnableBroadcast(enabled bool) {
	s.broadcast = enabled
}

func (s *DummyServer) SetEncoding(enc encoding.Encoding) error {
	return nil
}

func (s *DummyServer) Stop() error {
	return nil
}