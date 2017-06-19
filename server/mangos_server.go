package server

import (
	"errors"
	"fmt"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/helpers"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pub"
	"github.com/go-mangos/mangos/protocol/sub"
	"github.com/go-mangos/mangos/transport/tcp"
	"github.com/satori/go.uuid"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"golang.org/x/sync/syncmap"
	"github.com/go-mangos/mangos/protocol/pull"
)

type socketMap struct {
	KillChan        chan struct{}
	Sock            mangos.Socket
	OutboundStarted bool
}

// MangosServer provides a server implementation to provide as an anchor for a Mangos-based pub/sub network. It
// will open a return connection to each connected client to enable two-way publishing and relay published
// messages from clients to the rest of the network so it behaves in a similar way to a redis pub/sub system.
// The server can also publish messages, but does not have a subscription facility.
type MangosServer struct {
	listening             bool
	relay                 mangos.Socket
	inboundMessageClients syncmap.Map
	conf                  *MangosServerConf
	encoding              encoding.Encoding
	id                    string
	onPublishHook         PublishHook
}

// MangosServerConf provides the configuration details for a MangosServer
type MangosServerConf struct {
	Encoding                   encoding.Encoding
	listenOn                   string
	serverHostname 	           string
	disableConnectionsFromSelf bool
}

func newMangoConfig(listenOn string, disableConnectionsFromSelf bool) *MangosServerConf {
	return &MangosServerConf{
		listenOn:                   listenOn,
		disableConnectionsFromSelf: disableConnectionsFromSelf,
		Encoding:                   encoding.JSON,
	}
}

// init will set up the initial state of the server
func (s *MangosServer) Init(config interface{}) error {
	s.conf = config.(*MangosServerConf)
	s.inboundMessageClients = syncmap.Map{}
	s.SetEncoding(s.conf.Encoding)
	s.id = uuid.NewV4().String()

	return nil
}

func (s *MangosServer) GetID() string {
	return s.id
}

func (s *MangosServer) Connections() []string {
	c := make([]string, 0)
	s.inboundMessageClients.Range(func (key, value interface{}) bool {
		c = append(c, key.(string))
		return true
	})

	return c
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
		if err = s.handleRemoveConnection(data); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Error("Could not handle connection remove: ", err)
			return false
		}
	}

	return true
}

func (s *MangosServer) receiveAndRelay(sock *socketMap, id string) {
	var err error
	var msg []byte

	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosServer",
	}).Info("Ready to relay...")

	for {
		msg, err = sock.Sock.Recv()

		if err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Error("[ReceiveAndRelay] Cannot recv: ", err.Error())
			break
		}

		if pubErr := s.relay.Send(msg); pubErr != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Errorf("[ReceiveAndRelay] Failed relay: ", pubErr.Error())
		}
		log.Debug("[SERVER] Relayed: ", string(msg))

		if s.onPublishHook != nil {
			s.onPublishHook([]byte{}, msg)
			log.Debug("Server relaying from SERVER to HOOK: ", msg)
		}

	}
}

func (s *MangosServer) listenForMessagesToRelayForAddress(address string, killChan chan struct{}) error {
	sm, f := s.inboundMessageClients.Load(address)
	if !f {
		return fmt.Errorf("Address not found: %v", address)
	}

	sock, _ := sm.(*socketMap)
	if sock.OutboundStarted {
		return nil
	}

	sock.OutboundStarted = true

	s.inboundMessageClients.Store(address, sock)

	// Create and listen on the socket, make sure we can kill it
	go s.receiveAndRelay(sock, address)

	for {
		select {
		case <-killChan:
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServerClient",
			}).Info("Stopping relay listener for ", address)
			if err := sock.Sock.Close(); err != nil {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.MangosServerClient",
				}).Warning("Failed to close socket: ", err)
			}
			sm, f := s.inboundMessageClients.Load(address)
			if f {
				sock, _ := sm.(*socketMap)
				sock.OutboundStarted = false
				s.inboundMessageClients.Store(address, sock)
			}
			break
		default:
			continue
		}
	}
	return nil
}

var ConnectToSelf error = errors.New("Connect to self. Void.")

func (s *MangosServer) connectToClientForMessages(address string) (mangos.Socket, error) {
	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosServer",
	}).Info("New inbound connection from: ", address)

	var err error

	if s.conf.disableConnectionsFromSelf {
		ifaces, err := net.Interfaces()
		if err != nil {
			return nil, err
		}

		for _, i := range ifaces {
			addrs, err := i.Addrs()
			if err != nil {
				return nil, err
			}

			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}

				if strings.Contains(address, ip.String()) {
					log.WithFields(logrus.Fields{
						"prefix": "tcf.MangosServer",
					}).Info("Connection is from self! skipping.")
					return nil, ConnectToSelf
				}
			}
		}

		if s.conf.serverHostname != "" {
			tcpAddr, _ := net.ResolveTCPAddr("tcp", s.conf.serverHostname)
			if strings.Contains(address, tcpAddr.IP.String()) {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.MangosServer",
				}).Info("Connection is from self (public)! skipping.")
				return nil, ConnectToSelf
			}
		}


	}

	var cSock mangos.Socket
	if cSock, err = sub.NewSocket(); err != nil {
		return nil, fmt.Errorf("can't get new socket: %s", err.Error())
	}
	cSock.AddTransport(tcp.NewTransport())

	var e *url.URL
	if e, err = url.Parse(address); err != nil {
		return nil, err
	}

	u := helpers.ExtendedURL{URL: e}

	// This is very ugly
	serverURL, _ := url.Parse(s.conf.listenOn)
	sHelper := helpers.ExtendedURL{URL: serverURL}
	pAsInt, err := strconv.Atoi(sHelper.Port())
	if err != nil {
		return nil, err
	}

	p := pAsInt + 1

	// The return address must always be inbound port+1 in order to find the correct publisher
	returnAddress := fmt.Sprintf("%v://%v:%v", u.URL.Scheme, u.Hostname(), strconv.Itoa(p))
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

func (s *MangosServer) startPullListener() error {
	var sock mangos.Socket
	var err error
	var msg []byte

	var e *url.URL
	if e, err = url.Parse(s.conf.listenOn); err != nil {
		log.Error(err)
		return err
	}

	// Puller is on listen port +1
	u := helpers.ExtendedURL{URL: e}
	var p int
	if p, err = strconv.Atoi(u.Port()); err != nil {
		log.Error(err)
		return err
	}

	url := fmt.Sprintf("tcp://0.0.0.0:%v", p+1)

	if sock, err = pull.NewSocket(); err != nil {
		log.Error(err)
		return err
	}

	sock.AddTransport(tcp.NewTransport())
	if err = sock.Listen(url); err != nil {
		log.Fatal(err)
		return err
	}

	for {
		msg, err = sock.Recv()
		if err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Error("[Pull Handler] Cannot recv: ", err.Error())
			break
		}

		if pubErr := s.relay.Send(msg); pubErr != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.MangosServer",
			}).Errorf("[ReceiveAndRelay] Failed relay: ", pubErr.Error())
		}
		log.Debug("[SERVER] Relayed: ", string(msg))

		if s.onPublishHook != nil {
			s.onPublishHook([]byte{}, msg)
			log.Debug("Server relaying from SERVER to HOOK: ", msg)
		}
	}

	return nil
}

func (s *MangosServer) handleNewConnection(data mangos.Port) error {
	//killChan := make(chan struct{})
	tcpAddr, err := data.GetProp("REMOTE-ADDR")
	if err != nil {
		log.Error("Cannot get remote address: ", err)
		return err
	}

	// Only use IP in case of multiple subs?
	asNetAddr, _ := tcpAddr.(*net.TCPAddr)
	addr := fmt.Sprintf("tcp://%v", asNetAddr.IP.String())

	//addr := fmt.Sprintf("tcp://%v", tcpAddr.(*net.TCPAddr).String())

	_, f := s.inboundMessageClients.Load(addr)
	if f {
		log.Warning("--- Connection already tracked!")
		return nil
	}

	// Do not create a counter connection
	//cSock, err := s.connectToClientForMessages(addr)
	//if err != nil {
	//	// TODO: This should be a special case!
	//	if err != ConnectToSelf {
	//		return err
	//	}
	//
	//	return nil
	//}
	//
	//log.Warningf("New inbound connection: Adding %v to socket map", addr)
	//s.inboundMessageClients.Store(addr, &socketMap{KillChan: killChan, Sock: cSock})

	//go s.listenForMessagesToRelayForAddress(addr, killChan)

	return nil
}

func (s *MangosServer) handleRemoveConnection(data mangos.Port) error {
	tcpAddr, err := data.GetProp("REMOTE-ADDR")
	if err != nil {
		log.Error("Cannot get remote address: ", err)
		return err
	}

	// addr := fmt.Sprintf("tcp://%v", tcpAddr.(*net.TCPAddr).String())

	// Only use IP in case of multiple subs?
	asNetAddr, _ := tcpAddr.(*net.TCPAddr)
	addr := fmt.Sprintf("tcp://%v", asNetAddr.IP.String())

	log.WithFields(logrus.Fields{
		"prefix": "tcf.MangosServer",
	}).Info("Closed inbound connection: ", addr)

	cs, f := s.inboundMessageClients.Load(addr)
	defer s.inboundMessageClients.Delete(addr)

	cSock, _ := cs.(*socketMap)
	if !f {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.MangosServer",
		}).Warning("--> Inbound connection not found in socket map: ", addr)
		return nil
	}

	// Don't block on send
	select {
	case cSock.KillChan <- struct{}{}:
		log.Warning("Closing socket")
		if err := cSock.Sock.Close(); err != nil {
			log.Error(err)
		}
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

		// Use a pipeline for inbound messages
		go s.startPullListener()

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

func (s *MangosServer) Publish(filter string, payload payloads.Payload) error {
	return s.doPublish(filter, payload, true)
}

func (s *MangosServer) Relay(filter string, payload payloads.Payload) error {
	return s.doPublish(filter, payload, false)
}

// Publish will send a Payload from the server to connected clients on the specified topic
func (s *MangosServer) doPublish(filter string, payload payloads.Payload, withHook bool) error {
	if payload == nil {
		return nil
	}

	payload.SetTopic(filter)

	if payload.From() == "" {
		payload.SetFrom(s.GetID())
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

	if withHook {
		// DEBUG: This is not causing duplicates - confirmed
		if s.onPublishHook != nil {
			s.onPublishHook([]byte(filter), asPayload)
			log.Debug("Server relaying from SERVER to HOOK: ", payload.GetID())

		}
	}

	return nil
}

func (s *MangosServer) SetOnPublish(onPublishHook PublishHook) error {
	s.onPublishHook = onPublishHook
	return nil
}
