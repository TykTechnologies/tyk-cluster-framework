package client

import (
	"errors"
	"github.com/TykTechnologies/logrus"
	"time"
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync"
	"github.com/TykTechnologies/tyk-cluster-framework/client/beacon"
	"runtime"
)

type payloadMap struct {
	mu sync.RWMutex
	payloadHandlers map[string]PayloadHandler
}

func (p *payloadMap) Add(filter string, handler PayloadHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.payloadHandlers[filter] = handler
}

func (p *payloadMap) Get(filter string) (PayloadHandler, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	handler, found := p.payloadHandlers[filter]
	return handler, found
}

type BeaconClient struct {
	ClientHandler
	Interval int
	Port int

	beacon *beacon.Beacon
	publishing bool
	hasStarted bool
	listening bool
	encoding Encoding
	payloadHandlers payloadMap
}

type BeaconTransmit struct {
	Channel string
	Transmit []byte
}

func (b *BeaconClient) Connect() error {
	// No op, UDP is passive
	return nil
}

func (b *BeaconClient) Publish(filter string, p Payload) error {
	if p == nil {
		b.beacon.Silence()
		b.publishing = false
		return nil
	}

	if b.publishing {
		return nil
	}

	if TCFConfig.SetEncodingForPayloadsGlobally {
		b.SetEncoding(b.encoding)
	}

	data, encErr := Marshal(p, b.encoding)
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

	asPayload := BeaconTransmit{
		Channel: filter,
		Transmit: encodedPayload,
	}

	wrappedSend, encErr := msgpack.Marshal(asPayload)
	if encErr != nil {
		return encErr
	}


	if len(wrappedSend) == 0 {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.beaconclient",
		}).Error("No data to send, not sending")
		return nil
	}

	// We're just changing the payload, so don't start another publish.
	if b.hasStarted {
		b.beacon.Restart(wrappedSend)
		return nil
	}

	pubErr := b.beacon.Publish(wrappedSend)
	if pubErr != nil {
		return pubErr
	}

	b.publishing = true
	b.hasStarted = true
	return nil
}

func (b *BeaconClient) registerHandlerForChannel(filter string, handler PayloadHandler) {
	log.WithFields(logrus.Fields{
		"prefix": "tcf.beaconclient",
	}).Debugf("Adding handler for: %v\n", filter)
	b.payloadHandlers.Add(filter, handler)
	log.WithFields(logrus.Fields{
		"prefix": "tcf.beaconclient",
	}).Debugf("Done adding handler for: %v\n", filter)
}

func (b *BeaconClient) handleBeaconMessage(s *beacon.Signal) {
	var beaconMsg BeaconTransmit

	decErr := msgpack.Unmarshal(s.Transmit, &beaconMsg)
	if decErr != nil {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.beaconclient",
		}).Errorf("Beacon decode error! %v\n", decErr)
		return
	}

	handler, found := b.payloadHandlers.Get(beaconMsg.Channel)
	if !found {
		return
	}

	b.HandleRawMessage(beaconMsg.Transmit, handler, b.encoding)
}

func (b *BeaconClient) startListening() {
	b.beacon.Subscribe([]byte{})
	b.listening = true
	log.WithFields(logrus.Fields{
		"prefix": "tcf.beaconclient",
	}).Debug("Listening")
	for {
		s := <-b.beacon.Signals()
		signal := s.(*beacon.Signal)
		log.WithFields(logrus.Fields{
			"prefix": "tcf.beaconclient",
		}).Debug("Message received!")
		b.handleBeaconMessage(signal)

		//select {
		//// TODO: Stop
		//case s := <-b.beacon.Signals():
		//	signal := s.(*beacon.Signal)
		//	b.handleBeaconMessage(signal)
		//}
	}
	log.WithFields(logrus.Fields{
		"prefix": "tcf.beaconclient",
	}).Info("Stopped listening")
	b.listening = false
}

func (b *BeaconClient) Subscribe(filter string, handler PayloadHandler) error {
	b.registerHandlerForChannel(filter, handler)

	if b.listening {
		return nil
	}


	go b.startListening()
	return nil
}

func (b *BeaconClient) SetEncoding(enc Encoding) error {
	b.encoding = enc
	return nil
}

func (b *BeaconClient) Init(config interface{}) error {
	if runtime.GOOS == "windows" {
		log.Fatal("Beacon is not compatible with windows OS")
	}

	b.beacon = beacon.New()
	b.beacon.SetPort(b.Port).SetInterval(time.Duration(b.Interval) * time.Second)

	b.payloadHandlers = payloadMap{
		payloadHandlers: make(map[string]PayloadHandler),
	}

	return nil
}
