package client

import (
	"errors"
	"fmt"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pub"
	"github.com/go-mangos/mangos/protocol/sub"
	"github.com/go-mangos/mangos/transport/tcp"
	"net/url"
	"strconv"
	"sync"
	"time"
	"github.com/TykTechnologies/tyk-cluster-framework/helpers"
)

type socketPayloadHandler struct {
	socket  mangos.Socket
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
		socket:  socket,
		handler: handler,
	}
}

func (p *socketMap) Get(filter string) (mangos.Socket, PayloadHandler, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	h, found := p.payloadHandlers[filter]
	return h.socket, h.handler, found
}

// MangosClient is a wrapper around the Mangos framework and provides a simple way to create a pub/sub network where
// all clients can publish to a server and the server can publish to a client using topics
type MangosClient struct {
	ClientHandler
	URL string

	pubSock            mangos.Socket
	Encoding           encoding.Encoding
	payloadHandlers    socketMap
	broadcastKillChans map[string]chan struct{}
	SubscribeChan      chan string
}

// Init will initialise a MangosClient
func (m *MangosClient) Init(config interface{}) error {

	m.SubscribeChan = make(chan string)
	m.broadcastKillChans = make(map[string]chan struct{})
	m.payloadHandlers = socketMap{
		payloadHandlers: make(map[string]*socketPayloadHandler),
	}

	return nil
}

// Connect will connect a MangosClient to a MangosServer
func (m *MangosClient) Connect() error {

	m.startMessagePublisher()

	return nil
}

// Stop will stop the MangosClient and close open connections
func (m *MangosClient) Stop() error {
	var err error

	if err = m.pubSock.Close(); err != nil {
		return err
	}

	return nil
}

// Publish will publish a Payload to a topic, the underlying topology is handled by the library
func (m *MangosClient) Publish(filter string, payload payloads.Payload) error {
	if payload == nil {
		return nil
	}

	data, encErr := payloads.Marshal(payload, m.Encoding)
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
			"prefix": "tcf.MangosClient",
		}).Error("No data to send, not sending")
		return nil
	}

	if m.pubSock == nil {
		return errors.New("Publisher not initialised")
	}

	if pubErr := m.pubSock.Send(asPayload); pubErr != nil {
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
	log.Debug("[CLIENT] Listening on: ", channel)

	m.notifySub(channel)

	for {
		log.Debug("[CLIENT] Listening...")

		if msg, err = sock.Recv(); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosClient",
			}).Fatal("Cannot recv: ", err.Error())
		}

		log.Debug("[CLIENT] Received: raw data: ", string(msg))

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

// Subscribe will subscribe to a topic and attache a PayloadHandler, this is only available in the client
func (m *MangosClient) Subscribe(filter string, handler PayloadHandler) (chan string, error) {
	var sock mangos.Socket
	var err error

	if sock, err = sub.NewSocket(); err != nil {
		return m.SubscribeChan, fmt.Errorf("can't get new sub socket: %s", err.Error())
	}
	sock.AddTransport(tcp.NewTransport())

	log.Info("Dialing: ", m.URL)
	if err = sock.Dial(m.URL); err != nil {
		return m.SubscribeChan, fmt.Errorf("can't dial on sub socket: %s", err.Error())
	}

	err = sock.SetOption(mangos.OptionSubscribe, []byte(filter))
	//err = sock.SetOption(mangos.OptionSubscribe, []byte(""))
	if err != nil {
		return nil, err
	}

	m.registerHandlerForChannel(sock, filter, handler)

	go m.startListening(sock, filter)
	return m.SubscribeChan, nil
}

// Set Encoding will set the message encoding for payloads sent over the wire
func (m *MangosClient) SetEncoding(enc encoding.Encoding) error {
	m.Encoding = enc
	return nil
}

// Broadcast will send a payload at a predefined rate, call StopBroadcast() to halt.
func (m *MangosClient) Broadcast(filter string, payload payloads.Payload, interval int) error {
	_, found := m.broadcastKillChans[filter]
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
					"prefix": "tcf.MangosClient",
				}).Info("Stopping broadcast on: ", f)
				return
			case <-ticker:

				log.WithFields(logrus.Fields{
					"prefix": "tcf.MangosClient",
				}).Debug("Sending: ", p)

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

// StopBroadcast will stop a broadcasted payload.
func (m *MangosClient) StopBroadcast(f string) error {
	killChan, found := m.broadcastKillChans[f]
	if !found {
		return errors.New("Filter not broadcasting")
	}

	killChan <- struct{}{}
	return nil
}

func (m *MangosClient) startMessagePublisher() error {
	var err error
	if m.pubSock, err = pub.NewSocket(); err != nil {
		log.Errorf("can't get new pub socket: %s", err)
		return nil
	}

	var e *url.URL
	if e, err = url.Parse(m.URL); err != nil {
		return err
	}

	u := helpers.ExtendedURL{URL: e}

	var p int
	if p, err = strconv.Atoi(u.Port()); err != nil {
		log.Error(err)
		return err
	}

	// The return address must always be inbound port+1 in order to find the correct publisher
	returnAddress := fmt.Sprintf("tcp://0.0.0.0:%v", p+1)
	log.Info("Creating Publisher...")

	m.pubSock.AddTransport(tcp.NewTransport())
	if err = m.pubSock.Listen(returnAddress); err != nil {
		log.Errorf("can't listen on pub socket: %s", err.Error())
		return errors.New("Can't listen on pub socket")
	}

	log.Info("Setting port hook")
	m.pubSock.SetPortHook(m.onPortAction)

	log.Info("Created Publisher on: ", returnAddress)

	return nil
}

func (m *MangosClient) onPortAction(action mangos.PortAction, data mangos.Port) bool {
	fmt.Println(data.Address())
	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosClient",
	}).Info("New publish connection from: ", data.Address())

	return true
}
