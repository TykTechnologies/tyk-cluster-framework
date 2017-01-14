package server

import (
	"errors"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"strings"

	"github.com/TykTechnologies/logrus"
)

var log *logrus.Logger = logger.GetLogger()

// Server represents a server object that accepts connections for a queue service
type Server interface {
	Listen() error
	Publish(string, client.Payload) error
	EnableBroadcast(bool)
	SetEncoding(client.Encoding) error
	Init(interface{}) error
}

// NewServer will generate a new server object based on the enum provided.
func NewServer(connectionString string, baselineEncoding client.Encoding) (Server, error) {
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
