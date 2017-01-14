package client

import (
	"strings"
	"errors"
	"strconv"
	"github.com/TykTechnologies/logrus"
	"net/url"
)

// Client is a queue client managed by TCF
type Client interface {
	Connect() error
	Publish(string, Payload) error
	Subscribe(string, PayloadHandler) error
	SetEncoding(Encoding) error
	Init(interface{}) error
}

// NewClient will create a new client object based on the enum provided, the object will be pre-configured
// with the defaults needed and any custom configurations passed in for the type
func NewClient(connectionString string, baselineEncoding Encoding) (Client, error) {
	parts := strings.Split(connectionString, "://")
	if len(parts) < 2 {
		return nil, errors.New("Connection string not in the correct format, must be transport://server:port")
	}

	transport := parts[0]
	switch transport {
	case "dummy":
		log.WithFields(logrus.Fields{
			"prefix": "tcf",
		}).Info("Using dummy back-end")
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
		log.WithFields(logrus.Fields{
			"prefix": "tcf",
		}).Info("Using Redis back-end")
		c := &RedisClient{
			URL: connectionString,
		}
		c.SetEncoding(baselineEncoding)
		c.Init(nil)
		return c, nil
	case "beacon":
		log.WithFields(logrus.Fields{
			"prefix": "tcf",
		}).Info("Using Beacon back-end")

		URL, err := url.Parse(connectionString)
		if err != nil {
			return nil, err
		}

		parts := strings.Split(URL.Host, ":")
		if len(parts) < 2 {
			return nil, errors.New("No port specified")
		}

		interval := URL.Query().Get("interval")
		if interval == "" {
			interval = "10"
		}

		asInt, convErr := strconv.Atoi(interval)
		if convErr != nil {
			return nil, convErr
		}

		portAsInt, portConvErr := strconv.Atoi(parts[1])
		if portConvErr != nil {
			return nil, portConvErr
		}

		log.WithFields(logrus.Fields{
			"prefix": "tcf",
		}).Debugf("Port is: %v\n", parts[1])
		log.WithFields(logrus.Fields{
			"prefix": "tcf",
		}).Debugf("Interval is: %v\n", asInt)

		c := &BeaconClient{
			Port: portAsInt,
			Interval: asInt,
		}
		c.SetEncoding(baselineEncoding)
		c.Init(nil)
		return c, nil
	default:
		return nil, errors.New("No valid transport set.")
	}
}