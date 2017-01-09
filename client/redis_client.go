package client

import (
	"github.com/garyburd/redigo/redis"
	"errors"
	"time"
	"strings"
	"net/url"
	"github.com/TykTechnologies/logrus"
)

type RedisClient struct{
	ClientHandler
	URL string
	pool *redis.Pool
	encoding Encoding
}

func (c *RedisClient) Init(config interface{}) error {
	return nil
}

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

func (c *RedisClient) Publish(filter string, p Payload) error {
	if TCFConfig.SetEncodingForPayloadsGlobally {
		p.SetEncoding(c.encoding)
	}
	data, encErr := Marshal(p, c.encoding)
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
	conn.Do("PUBLISH", filter, string(toSend))
	return nil
}

func (c *RedisClient) Subscribe(filter string, handler PayloadHandler) error {

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
				c.HandleRawMessage(v.Data, handler, c.encoding)

			case redis.Subscription:
				log.WithFields(logrus.Fields{
					"prefix": "tcf.redisclient",
				}).Info("Subscription started: ", v.Channel)

			case error:
				log.WithFields(logrus.Fields{
					"prefix": "tcf.redisclient",
				}).Error("Redis disconnected: ", v)
			}
		}
		log.WithFields(logrus.Fields{
			"prefix": "tcf.redisclient",
		}).Warning("Connection closed")

	}(filter, handler)

	return nil
}

func (c *RedisClient) SetEncoding(enc Encoding) error {
	c.encoding = enc
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