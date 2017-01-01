package tcf

import (
	"strings"
	"errors"
	"strconv"
	"fmt"
)

// Client is a queue client managed by TCF
type Client interface {
	Connect() error
	Publish(string, Payload) error
	Subscribe(string, PayloadHandler) error
	SetEncoding(encoding) error
	Init(interface{}) error
}

// NewClient will create a new client object based on the enum provided, the object will be pre-configured
// with the defaults needed and any custom configurations passed in for the type
func NewClient(connectionString string, baselineEncoding encoding) (Client, error) {
	parts := strings.Split(connectionString, "://")
	if len(parts) < 2 {
		return nil, errors.New("Connection string not in the correct format, must be transport://server:port")
	}

	transport := parts[0]
	switch transport {
	case "dummy":
		connParts := strings.Split(parts[1], ":")
		if len(connParts) > 2 {
			return nil, errors.New("Detected IPv6 address, this is not supported yet")
		}

		portAsInt, convErr := strconv.Atoi(connParts[1])
		if convErr != nil {
			return nil, convErr
		}

		c := &DummyClient{
			Hostname: connParts[0],
			Port: portAsInt,
		}
		c.SetEncoding(baselineEncoding)
		c.Init(nil)
		return c, nil
	case "redis":
		fmt.Println("REDIS CLIENT")
		c := &RedisClient{
			URL: connectionString,
		}
		c.SetEncoding(baselineEncoding)
		c.Init(nil)
		return c, nil
	default:
		return nil, errors.New("No valid transport set.")
	}
}