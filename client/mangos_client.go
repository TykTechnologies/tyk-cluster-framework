package client

import (
	"errors"
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/transport/tcp"
	"github.com/go-mangos/mangos/protocol/sub"
	"fmt"
	"sync"
	"github.com/TykTechnologies/logrus"
	"time"
	"github.com/go-mangos/mangos/protocol/pub"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

type socketPayloadHandler struct {
	socket mangos.Socket
	handler PayloadHandler
}

type socketMap struct {
	mu              sync.RWMutex
	payloadHandlers map[string]*socketPayloadHandler
}

func (p *socketMap) Add(socket mangos.Socket, filter string, handler PayloadHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.payloadHandlers[filter] = &socketPayloadHandler{
		socket: socket,
		handler: handler,
	}
}

func (p *socketMap) Get(filter string) (mangos.Socket, PayloadHandler, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	h, found := p.payloadHandlers[filter]
	return h.socket, h.handler, found
}

type MangosClient struct {
	ClientHandler
	URL string

	sock 		mangos.Socket
	Encoding        encoding.Encoding
	payloadHandlers socketMap
	broadcastKillChans map[string]chan struct{}
	SubscribeChan chan string

}

func (m *MangosClient) Init(config interface{}) error {

	m.SubscribeChan = make(chan string)
	m.payloadHandlers = socketMap{
		payloadHandlers: make(map[string]*socketPayloadHandler),
	}

	var err error
	if m.sock, err = pub.NewSocket(); err != nil {
		return fmt.Errorf("can't get new socket: %s", err.Error())
	}
	m.sock.AddTransport(tcp.NewTransport())


	return nil
}

func (m *MangosClient) Connect() error {
	var err error
	if err = m.sock.Listen(m.URL); err != nil {
		return fmt.Errorf("can't dial on socket: %s", err.Error())
	}

	log.Info("Connected! ", m.URL)

	return nil
}

func (m *MangosClient) Stop() error {
	return m.sock.Close()
}

func (m *MangosClient) Publish(filter string, payload Payload) error {
	if payload == nil {
		return nil
	}

	data, encErr := Marshal(payload, m.Encoding)
	if encErr != nil {
		return encErr
	}

	var encodedPayload []byte
	switch data.(type) {
	case []byte:
		encodedPayload = data.([]byte)
		break
	case string:
		encodedPayload = []byte(data.(string))
		break
	default:
		return errors.New("Encoded data is not supported")
	}

	asPayload := append([]byte(filter), encodedPayload...)

	if len(asPayload) == 0 {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.mangos-server",
		}).Error("No data to send, not sending")
		return nil
	}

	if pubErr := m.sock.Send(asPayload); pubErr != nil {
		return fmt.Errorf("Failed publishing: %s", pubErr.Error())
	}

	return nil
}

func (m *MangosClient) registerHandlerForChannel(socket mangos.Socket, filter string, handler PayloadHandler) {
	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosClient",
	}).Debugf("Adding handler for: %v\n", filter)

	m.payloadHandlers.Add(socket, filter, handler)

	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosClient",
	}).Debugf("Done adding handler for: %v\n", filter)
}

func (m *MangosClient) notifySub(channel string) {
	select {
	case m.SubscribeChan <- channel:
	default:
	}
}

func (m *MangosClient) startListening(sock mangos.Socket, channel string) {
	var msg []byte
	var err error
	log.Info("Listening on: ", channel)

	m.notifySub(channel)

	for {

		if msg, err = sock.Recv(); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosClient",
			}).Fatalf("Cannot recv: %s\n", err.Error())
		}

		log.Debug("Received: raw data: ", string(msg))

		// Strip the namespace
		payload := msg[len(channel):]

		log.Debug("Received: stripped data: ", string(payload))

		_, handler, found := m.payloadHandlers.Get(channel)
		if found {
			log.Debug("Found handler for: ", channel)
			handlingErr := m.HandleRawMessage(payload, handler, m.Encoding)
			if handlingErr != nil {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.MangosClient",
				}).Error("Failed to handle message: ", handlingErr)
			}
		}


	}

}

func (m *MangosClient) Subscribe(filter string, handler PayloadHandler) (chan string, error) {
	var sock mangos.Socket
	var err error

	if sock, err = sub.NewSocket(); err != nil {
		return m.SubscribeChan, fmt.Errorf("can't get new sub socket: %s", err.Error())
	}
	sock.AddTransport(tcp.NewTransport())
	if err = sock.Dial(m.URL); err != nil {
		return m.SubscribeChan, fmt.Errorf("can't dial on sub socket: %s", err.Error())
	}
	// Empty byte array effectively subscribes to everything
	err = sock.SetOption(mangos.OptionSubscribe, []byte(filter))

	m.registerHandlerForChannel(sock, filter, handler)

	go m.startListening(sock, filter)
	return m.SubscribeChan, nil
}

func (m *MangosClient) SetEncoding(enc encoding.Encoding) error {
	m.Encoding = enc
	return nil
}

func (m *MangosClient) Broadcast(filter string, payload Payload, interval int) error {
	_, found := m.broadcastKillChans[filter]
	if found {
		return errors.New("Filter already broadcasting, stop first")
	}

	killChan := make(chan struct{})
	go func(f string, p Payload, i int, k chan struct{}) {
		var ticker <-chan time.Time
		ticker = time.After(time.Duration(i) * time.Second)

		for {
			select {
			case <- k:
				// Kill broadcast
				log.WithFields(logrus.Fields{
					"prefix": "tcf.MangosClient",
				}).Info("Stopping broadcast on: ", f)
				return
			case <- ticker:

				log.WithFields(logrus.Fields{
					"prefix": "tcf.MangosClient",
				}).Debug ("Sending: ", p)

				if pErr := m.Publish(f, p); pErr != nil {
					log.WithFields(logrus.Fields{
						"prefix": "tcf.MangosClient",
					}).Error("Failed to broadcast: ", pErr)
				}
				ticker = time.After(time.Duration(i) * time.Second)
			}
		}

	}(filter, payload, interval, killChan)

	m.broadcastKillChans[filter] = killChan
	return nil
}

func (m *MangosClient) StopBroadcast (f string) error {
	killChan, found := m.broadcastKillChans[f]
	if !found {
		return errors.New("Filter not broadcasting")
	}

	killChan <- struct{}{}
	return nil
}
