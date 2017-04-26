package client

import (
	"errors"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"net/url"
	"strconv"
	"strings"
)

// Client is a queue client managed by TCF
type Client interface {
	Connect() error
	Publish(string, payloads.Payload) error
	Subscribe(string, PayloadHandler) (chan string, error)
	Broadcast(string, payloads.Payload, int) error
	StopBroadcast(string) error
	SetEncoding(encoding.Encoding) error
	Init(interface{}) error
	Stop() error
	SetConnectionDropHook(func() error) error
}

// NewClient will create a new client object based on the enum provided, the object will be pre-configured
// with the defaults needed and any custom configurations passed in for the type.
// For `beacon`, it is possible to set an `?interval=time_in_ms` option to set the broadcast interval.
// For `mangos`, it is possible to set an `?disable_publisher` boolean that stops the client from creating
// a publishing channel, this is useful for servers that run their own clients to subscribe to themselves.
// Should be used in conjunction with the `disable_loopback` option in the server.
func NewClient(connectionString string, baselineEncoding encoding.Encoding) (Client, error) {
	parts := strings.Split(connectionString, "://")
	if len(parts) < 2 {
		return nil, errors.New("Connection string not in the correct format, must be transport://server:port")
	}

	transport := parts[0]
	switch transport {
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
			Port:     portAsInt,
			Interval: asInt,
		}
		c.SetEncoding(baselineEncoding)
		c.Init(nil)
		return c, nil

	case "mangos":
		log.WithFields(logrus.Fields{
			"prefix": "tcf",
		}).Info("Using Mangos back-end")

		URL, err := url.Parse(connectionString)
		if err != nil {
			return nil, err
		}

		parts := strings.Split(URL.Host, ":")
		if len(parts) < 2 {
			return nil, errors.New("No port specified")
		}
		url := "tcp://" + URL.Host
		log.WithFields(logrus.Fields{
			"prefix": "tcf",
		}).Info("Connecting to: ", url)

		disablePublisher := URL.Query().Get("disable_publisher")

		c := &MangosClient{
			URL:              url,
			disablePublisher: disablePublisher != "",
		}

		c.SetEncoding(baselineEncoding)
		if initErr := c.Init(nil); initErr != nil {
			return nil, initErr
		}

		return c, nil
	default:
		return nil, errors.New("No valid transport set.")
	}
}
