package tcf

import "fmt"

type DummyServer struct {
	broadcast bool
}

func (s *DummyServer) Init(config interface{}) error {
	return nil
}

func (s *DummyServer) Listen() error {
	return nil
}

func (s *DummyServer) Publish(filter string, p Payload) error {
	payload := make(map[string]interface{})
	decErr := p.DecodeMessage(&payload)
	if decErr != nil {
		fmt.Printf("Error: decoding failed: %v\n", decErr)
	}

	fmt.Printf("PUBLISHING: %v \n", payload)

	return nil
}

func (s *DummyServer) EnableBroadcast(enabled bool) {
	s.broadcast = enabled
}

func (s *DummyServer) SetEncoding(enc encoding) error {
	return nil
}