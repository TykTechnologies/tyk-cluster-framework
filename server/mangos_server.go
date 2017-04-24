package server

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
	"time"
	"github.com/TykTechnologies/tyk-cluster-framework/helpers"
)

type socketMap struct {
	KillChan chan struct{}
	Sock     mangos.Socket
}

// MangosServer provides a server implementation to provide as an anchor for a Mangos-based pub/sub network. It
// will open a return connection to each connected client to enable two-way publishing and relay published
// messages from clients to the rest of the network so it behaves in a similar way to a redis pub/sub system.
// The server can also publish messages, but does not have a subscription facility.
type MangosServer struct {
	listening             bool
	relay                 mangos.Socket
	inboundMessageClients map[string]*socketMap
	conf                  *MangosServerConf
	encoding              encoding.Encoding
}

// MangosServerConf provides the configuration details for a MangosServer
type MangosServerConf struct {
	Encoding encoding.Encoding
	listenOn string
}

func newMangoConfig(listenOn string) *MangosServerConf {
	return &MangosServerConf{listenOn: listenOn, Encoding: encoding.JSON}
}

// init will set up the initial state of the server
func (s *MangosServer) Init(config interface{}) error {
	s.conf = config.(*MangosServerConf)
	s.inboundMessageClients = make(map[string]*socketMap)
	s.SetEncoding(s.conf.Encoding)

	return nil
}

// Connections returns a list of connected clients, used mainly in testing
func (s *MangosServer) Connections() []string {
	conns := make([]string, len(s.inboundMessageClients))
	c := 0
	for addr, _ := range s.inboundMessageClients {
		conns[c] = addr
		c += 1
	}

	return conns
}

func (s *MangosServer) startListening() error {
	var err error
	if s.relay, err = pub.NewSocket(); err != nil {
		return fmt.Errorf("can't get new pub socket: %s", err)
	}

	s.relay.AddTransport(tcp.NewTransport())
	if err = s.relay.Listen(s.conf.listenOn); err != nil {
		return fmt.Errorf("can't listen on pub socket: %s", err.Error())
	}

	// on connect we need to set up relay
	s.relay.SetPortHook(s.onPortAction)

	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosServer",
	}).Info("Server listening on: ", s.conf.listenOn)
	return nil
}

func (s *MangosServer) onPortAction(action mangos.PortAction, data mangos.Port) bool {
	var err error

	switch action {
	case mangos.PortActionAdd:
		if err = s.handleNewConnection(data); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Error("Could not handle conneciton add: ", err)
			return false
		}
	case mangos.PortActionRemove:
		log.WithFields(logrus.Fields{
			"prefix": "tcf.MangosServer",
		}).Debug("Closed inbound connection: ", data.Address())
		if err = s.handleRemoveConnection(data); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Error("Could not handle conneciton remove: ", err)
			return false
		}
	}

	return true
}

func (s *MangosServer) receiveAndRelay(sock *socketMap) {
	var err error
	var msg []byte

	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosServer",
	}).Info("Ready to relay...")

	for {
		msg, err = sock.Sock.Recv()

		fmt.Println(msg)

		if err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Error("Cannot recv: ", err.Error())
			break
		}

		if pubErr := s.relay.Send(msg); pubErr != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Errorf("Failed relay: ", pubErr.Error())
		}
		log.Debug("[SERVER] Relayed: ", string(msg))

	}
}

func (s *MangosServer) listenForMessagesToRelayForAddress(address string, killChan chan struct{}) error {
	sock, f := s.inboundMessageClients[address]
	if !f {
		return fmt.Errorf("Address not found: %v", address)
	}

	// Create and listen on the socket, make sure we can kill it
	go s.receiveAndRelay(sock)

	for {
		select {
		case <-killChan:
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServerClient",
			}).Debug("Stopping relay listener for ", address)
			if err := sock.Sock.Close(); err != nil {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.MangosServerClient",
				}).Warning("Failed to close socket: ", err)
			}
			break
		default:
			continue
		}
	}
	return nil
}

func (s *MangosServer) connectToClientForMessages(address string) (mangos.Socket, error) {
	var cSock mangos.Socket
	var err error
	if cSock, err = sub.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new socket: %s", err.Error())
	}
	cSock.AddTransport(tcp.NewTransport())

	var e *url.URL
	if e, err = url.Parse(address); err != nil {
		return nil, err
	}

	u := helpers.ExtendedURL{URL: e}

	var p int
	if p, err = strconv.Atoi(u.Port()); err != nil {
		return nil, err
	}


	// The return address must always be inbound port+1 in order to find the correct publisher
	// TODO: This fails in a docker environment. But works if a hostname is used instead of an IP address
	returnAddress := fmt.Sprintf("%v://%v:%v", u.URL.Scheme, u.Hostname(), p+1)
	log.Info("DIALING: ", returnAddress)
	if err = cSock.Dial(returnAddress); err != nil {
		return nil, fmt.Errorf("can't dial out on socket: %s", err.Error())
	}

	err = cSock.SetOption(mangos.OptionSubscribe, []byte(""))
	if err != nil {
		return nil, err
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosServer",
	}).Debug("Connecting relay to inbound client: ", returnAddress)

	return cSock, nil
}

func (s *MangosServer) handleNewConnection(data mangos.Port) error {
	_, f := s.inboundMessageClients[data.Address()]
	if f {
		return nil
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosServer",
	}).Info("New inbound connection from: ", data.Address())

	killChan := make(chan struct{})
	cSock, err := s.connectToClientForMessages(data.Address())
	if err != nil {
		return err
	}
	s.inboundMessageClients[data.Address()] = &socketMap{KillChan: killChan, Sock: cSock}
	go s.listenForMessagesToRelayForAddress(data.Address(), killChan)

	return nil
}

func (s *MangosServer) handleRemoveConnection(data mangos.Port) error {
	cSock, f := s.inboundMessageClients[data.Address()]
	if !f {
		return nil
	}

	// Don't block on send
	select {
	case cSock.KillChan <- struct{}{}:
		delete(s.inboundMessageClients, data.Address())
	case <-time.After(time.Millisecond * 500):
		return errors.New("Failed to stop listener for leaving client")
	}

	return nil
}

func (s *MangosServer) Listen() error {
	if s.listening == false {
		err := s.startListening()
		if err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Fatal(err)
		}
		s.listening = true
		return nil
	}

	return errors.New("Already listening")
}

// Server does not broadcast
func (s *MangosServer) EnableBroadcast(enabled bool) {
	// no op
}

// SetEncoding will set the encoding to use on published payloads
func (s *MangosServer) SetEncoding(enc encoding.Encoding) error {
	s.encoding = enc
	return nil
}

// Stop will stop the server
func (s *MangosServer) Stop() error {
	if s.listening {
		return s.relay.Close()
	}
	return errors.New("Already stopped")
}

// Publish will send a Payload from the server to connected clients on the specified topic
func (s *MangosServer) Publish(filter string, payload payloads.Payload) error {
	if payload == nil {
		return nil
	}

	data, encErr := payloads.Marshal(payload, s.encoding)
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

	if pubErr := s.relay.Send(asPayload); pubErr != nil {
		return fmt.Errorf("Failed publishing: %s", pubErr.Error())
	}

	return nil
}
