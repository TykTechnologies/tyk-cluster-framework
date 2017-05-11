package server

import (
	"errors"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"strings"

	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"net/url"
)

var log *logrus.Logger = logger.GetLogger()

type PublishHook func([]byte, []byte) error

// Server represents a server object that accepts connections for a queue service
type Server interface {
	Listen() error
	EnableBroadcast(bool)
	SetEncoding(encoding.Encoding) error
	Init(interface{}) error
	Stop() error
	Connections() []string
	Publish(string, payloads.Payload) error
	Relay(string, payloads.Payload) error
	GetID() string
	SetOnPublish(PublishHook) error
}

// NewServer will generate a new server object based on the enum provided.
// 'mangos' will enable a server using the mangos (nanomsg) toolkit, you
// can add an option to the connection string of `?disable_loopback=true`
// to have the server dissallow connections from any IP that it recognises
// as itself.
func NewServer(connectionString string, baselineEncoding encoding.Encoding) (Server, error) {
	parts := strings.Split(connectionString, "://")
	if len(parts) < 2 {
		return nil, errors.New("Connection string not in the correct format, must be transport://server:port?option=value")
	}

	transport := parts[0]
	switch transport {
	case "mangos":

		URL, err := url.Parse(connectionString)
		if err != nil {
			return nil, err
		}

		parts := strings.Split(URL.Host, ":")
		if len(parts) < 2 {
			return nil, errors.New("No port specified")
		}

		disableConnectionsFromSelf := URL.Query().Get("disable_loopback")

		s := &MangosServer{}
		s.SetEncoding(baselineEncoding)
		url := "tcp://" + URL.Host
		s.Init(newMangoConfig(url, disableConnectionsFromSelf != ""))
		return s, nil
	default:
		return nil, errors.New("Server scheme not supported.")
	}
}
