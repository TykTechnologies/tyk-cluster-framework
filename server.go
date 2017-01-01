package tcf

import (
	"strings"
	"errors"
)

// Se3rver represents a server object that accepts connections for a queue service
type Server interface {
	Listen() error
	Publish(string, Payload) error
	EnableBroadcast(bool)
	SetEncoding(encoding) error
	Init(interface{}) error
}

// NewServer will generate a new server object based on the enum provided.
func NewServer(connectionString string, baselineEncoding encoding) (Server, error) {
	parts := strings.Split(connectionString, "://")
	if len(parts) < 2 {
		return nil, errors.New("Connection string not in the correct format, must be transport://server:port")
	}

	transport := parts[0]
	switch transport {
	case "dummy":
		s := &DummyServer{}
		s.SetEncoding(baselineEncoding)
		s.Init(nil)
		return s, nil
	default:
		s := &DummyServer{}
		s.SetEncoding(baselineEncoding)
		return s, nil
	}
}