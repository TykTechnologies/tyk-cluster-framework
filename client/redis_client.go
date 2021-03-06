package client

import (
	"errors"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"github.com/garyburd/redigo/redis"
	"net/url"
	"strings"
	"time"
)

// RedisClient provides an abstraction over redis' pub/sub mechanism, it only works with
// redis at the moment, not redis cluster, however this should be ok as redis cluster
// does not cluster pub/sub.
type RedisClient struct {
	ClientHandler
	URL                string
	pool               *redis.Pool
	Encoding           encoding.Encoding
	broadcastKillChans map[string]chan struct{}
	SubscribeChan      chan string
	id                 string
}

// Init will initialise the redis client
func (c *RedisClient) Init(config interface{}) error {
	c.broadcastKillChans = make(map[string]chan struct{})
	c.SubscribeChan = make(chan string)
	return nil
}

// Stop will close all redis connections
func (c *RedisClient) Stop() error {
	return c.pool.Close()
}

func (c *RedisClient) GetID() string {
	return c.id
}

// Connect will set up the redis connection
func (c *RedisClient) Connect() error {
	if c.URL == "" {
		return errors.New("Redis URL not set!!")
	}

	var err error
	c.pool, err = c.setupRedisPool(c.URL)
	if err != nil {
		return err
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.redisclient",
	}).Info("Connected: ", c.URL)
	return nil
}

// Publish will publish a Payload on the redis pub/sub channel
func (c *RedisClient) Publish(filter string, p payloads.Payload) error {
	if TCFConfig.SetEncodingForPayloadsGlobally {
		p.SetEncoding(c.Encoding)
	}
	data, encErr := payloads.Marshal(p, c.Encoding)
	if encErr != nil {
		return encErr
	}

	var toSend string
	switch data.(type) {
	case []byte:
		toSend = string(data.([]byte))
		break
	case string:
		toSend = data.(string)
		break
	default:
		return errors.New("Encoded data is not supported")
	}

	if len(toSend) == 0 {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.redisclient",
		}).Error("No data to send, not sending")
		return nil
	}

	conn := c.pool.Get()
	defer conn.Close()

	if conn == nil {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.redisclient",
		}).Warning("Not connected, connecting")
		c.Connect()
	}

	//fmt.Printf("REDIS PUBLISHING: %v\n", string(toSend))
	conn.Do("PUBLISH", filter, string(toSend))
	return nil
}

func (c *RedisClient) notifySub(channel string) {
	select {
	case c.SubscribeChan <- channel:
	default:
	}
}

// Subscribe will create a subscription on the redis topic and attach a handler
func (c *RedisClient) Subscribe(filter string, handler PayloadHandler) (chan string, error) {

	// Create a subscription and a hold loop, the outer loop is to re-create the object if it breaks.
	go func(filter string, handler PayloadHandler) {

		conn := c.pool.Get()
		defer conn.Close()

		if conn == nil {
			log.Println("Not connected, connecting")
			c.Connect()
		}

		psc := redis.PubSubConn{Conn: conn}
		psc.Subscribe(filter)

		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				c.HandleRawMessage(v.Data, handler, c.Encoding)

			case redis.Subscription:
				log.WithFields(logrus.Fields{
					"prefix": "tcf.redisclient",
				}).Info("Subscription started: ", v.Channel)
				c.notifySub(filter)

			case error:
				log.WithFields(logrus.Fields{
					"prefix": "tcf.redisclient",
				}).Error("Redis disconnected: ", v)
				break
			}
		}
		log.WithFields(logrus.Fields{
			"prefix": "tcf.redisclient",
		}).Warning("Connection closed")

	}(filter, handler)

	return c.SubscribeChan, nil
}

// SetEncoding sets the payload encoding to use when moving messages around
func (c *RedisClient) SetEncoding(enc encoding.Encoding) error {
	c.Encoding = enc
	return nil
}

func (c *RedisClient) setupRedisPool(s string) (*redis.Pool, error) {
	redisURL, err := url.Parse(s)

	if err != nil {
		return nil, err
	}

	auth := ""

	if redisURL.User != nil {
		if password, ok := redisURL.User.Password(); ok {
			auth = password
		}
	}

	var MaxActive, MaxIdle, IdleTimeout int = 500, 1000, 240
	if TCFConfig.Handlers.Redis.MaxIdle > 0 {
		MaxIdle = TCFConfig.Handlers.Redis.MaxIdle
	}
	if TCFConfig.Handlers.Redis.MaxActive > 0 {
		MaxActive = TCFConfig.Handlers.Redis.MaxActive
	}
	if TCFConfig.Handlers.Redis.IdleTimeout > 0 {
		IdleTimeout = TCFConfig.Handlers.Redis.IdleTimeout
	}

	thisClientPool := &redis.Pool{
		MaxIdle:     MaxIdle,
		MaxActive:   MaxActive,
		IdleTimeout: time.Duration(IdleTimeout) * time.Second,
		Dial: func() (redis.Conn, error) {
			rc, err := redis.Dial("tcp", redisURL.Host)
			if err != nil {
				return nil, err
			}
			if len(auth) > 0 {
				if _, err := rc.Do("AUTH", auth); err != nil {
					rc.Close()
					return nil, err
				}
			}
			if len(redisURL.Path) > 1 {
				db := strings.TrimPrefix(redisURL.Path, "/")

				if _, err := rc.Do("SELECT", db); err != nil {
					rc.Close()
					return nil, err
				}
			}
			return rc, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return thisClientPool, nil
}

// Broadcast will publish a periodic message to a topic at a preset interval
func (c *RedisClient) Broadcast(filter string, payload payloads.Payload, interval int) error {
	_, found := c.broadcastKillChans[filter]
	if found {
		return errors.New("Filter already broadcasting, stop first")
	}

	killChan := make(chan struct{})
	go func(f string, p payloads.Payload, i int, k chan struct{}) {
		var ticker <-chan time.Time
		ticker = time.After(time.Duration(i) * time.Second)

		for {
			select {
			case <-k:
				// Kill broadcast
				log.WithFields(logrus.Fields{
					"prefix": "tcf.redisclient",
				}).Info("Stopping broadcast on: ", f)
				return
			case <-ticker:

				log.WithFields(logrus.Fields{
					"prefix": "tcf.redisclient",
				}).Debug("Sending: ", p)

				if pErr := c.Publish(f, p); pErr != nil {
					log.WithFields(logrus.Fields{
						"prefix": "tcf.redisclient",
					}).Error("Failed to broadcast: ", pErr)
				}
				ticker = time.After(time.Duration(i) * time.Second)
			}
		}

	}(filter, payload, interval, killChan)

	c.broadcastKillChans[filter] = killChan
	return nil
}

// StopBroadcast will stop a broadcast
func (c *RedisClient) StopBroadcast(f string) error {
	killChan, found := c.broadcastKillChans[f]
	if !found {
		return errors.New("Filter not broadcasting")
	}

	killChan <- struct{}{}
	return nil
}

func (c *RedisClient) SetConnectionDropHook(callback func() error) error {
	// TODO: Implement disconnect detection
	return nil
}
