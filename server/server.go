package server

import (
	"errors"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"strings"

	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

var log *logrus.Logger = logger.GetLogger()

// Server represents a server object that accepts connections for a queue service
type Server interface {
	Listen() error
	EnableBroadcast(bool)
	SetEncoding(encoding.Encoding) error
	Init(interface{}) error
	Stop() error
	Connections() []string
}

// NewServer will generate a new server object based on the enum provided.
func NewServer(connectionString string, baselineEncoding encoding.Encoding) (Server, error) {
	parts := strings.Split(connectionString, "://")
	if len(parts) < 2 {
		return nil, errors.New("Connection string not in the correct format, must be transport://server:port")
	}

	transport := parts[0]
	switch transport {
	case "mangos":
		s := &MangosServer{}
		s.SetEncoding(baselineEncoding)
		url := "tcp://" + parts[1]
		s.Init(newMangoConfig(url))
		return s, nil
	default:
		return nil, errors.New("Server scheme not supported.")
	}
}
