package client

import (
	"errors"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/client/beacon"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"gopkg.in/vmihailenco/msgpack.v2"
	"runtime"
	"sync"
	"time"
)

// payloadMap is a map that connects channel filter names to handlers so that
// multiple payload handlers can be attached to a filter.
type payloadMap struct {
	mu              sync.RWMutex
	payloadHandlers map[string]PayloadHandler
}

// Add a payload handler to a filter
func (p *payloadMap) Add(filter string, handler PayloadHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.payloadHandlers[filter] = handler
}

// Get a payload handler from a filter
func (p *payloadMap) Get(filter string) (PayloadHandler, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	handler, found := p.payloadHandlers[filter]
	return handler, found
}

// BeaconClient is a wrapper around the beacon library \
// for Gyre (https://github.com/zeromq/gyre/blob/master/beacon/beacon.go), the file has been
// internalised here so that some modifications could be made.
//
// The BeaconClient will transmit a payload over UDP at a pre-defined interval, and can
// support multiple filters and payload handlers, Beacon will only Subscribe and Broadcast,
// the "Publish" method is not implemented because it would not make sense with regards to how
// Beacon is implemented.
//
// BeaconClient is not compatible with Windows hosts (yet).
//
// For a usage example see the `examples/beacon_broadcast/beacon_example.go` file.
type BeaconClient struct {
	ClientHandler
	Interval      int
	Port          int
	SubscribeChan chan string

	beacon          *beacon.Beacon
	publishing      bool
	hasStarted      bool
	listening       bool
	Encoding        encoding.Encoding
	payloadHandlers payloadMap
}

// The default Beacon payload format, because we need to handle channel subscriptions manually.
type BeaconTransmit struct {
	Channel  string
	Transmit []byte
}

// Stop will stop the client
func (b *BeaconClient) Stop() error {
	b.beacon.Close()
	return nil
}

// Connect is not implemented
func (b *BeaconClient) Connect() error {
	// No op, UDP is passive
	return nil
}

// Publish is not implemented because the underlying beacon
// library is designed for broadcasting periodically. Use the `Broadcast` method instead.
func (b *BeaconClient) Publish(filter string, p payloads.Payload) error {
	return errors.New("Beacon only broadcasts and subscribes")
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

	b.HandleRawMessage(beaconMsg.Transmit, handler, b.Encoding)
}

func (b *BeaconClient) startListening(filter string) {
	b.beacon.Subscribe([]byte{})
	b.listening = true

	select {
	case b.SubscribeChan <- filter:
		log.WithFields(logrus.Fields{
			"prefix": "tcf.beaconclient",
		}).Info("Subscription notification sent")
	default:
		log.WithFields(logrus.Fields{
			"prefix": "tcf.beaconclient",
		}).Debug("Subscription notification failed to send, continuing")
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.beaconclient",
	}).Debug("Listening")

	for {
		s := <-b.beacon.Signals()
		if s != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.beaconclient",
			}).Debug("Message received: ", string(s.(*beacon.Signal).Transmit))
			b.handleBeaconMessage(s.(*beacon.Signal))
		}

	}
}

// Subscribe enables you to add a payload handler to a chanel filter, so multiple functions
// can be set against different channels. Wildcards are not supported.
func (b *BeaconClient) Subscribe(filter string, handler PayloadHandler) (chan string, error) {
	b.registerHandlerForChannel(filter, handler)

	if b.listening {
		return b.SubscribeChan, nil
	}

	go b.startListening(filter)
	return b.SubscribeChan, nil
}

// SetEncoding Will set the encoding of the payloads to be sent and received.
func (b *BeaconClient) SetEncoding(enc encoding.Encoding) error {
	b.Encoding = enc
	return nil
}

// Init initialises the `BeaconClient`, it is called automatically by `NewClient()`
// if the prefix of the connection string is `beacon://`
func (b *BeaconClient) Init(config interface{}) error {
	if runtime.GOOS == "windows" {
		log.Fatal("Beacon is not compatible with windows OS")
	}

	b.beacon = beacon.New()
	b.beacon.SetPort(b.Port).SetInterval(time.Duration(b.Interval) * time.Second)
	b.SubscribeChan = make(chan string)

	b.payloadHandlers = payloadMap{
		payloadHandlers: make(map[string]PayloadHandler),
	}

	return nil
}

// Broadcast will send the set payload via UDP every interval to the specified channel.
func (b *BeaconClient) Broadcast(filter string, payload payloads.Payload, interval int) error {

	if payload == nil {
		b.beacon.Silence()
		b.publishing = false
		return nil
	}

	if TCFConfig.SetEncodingForPayloadsGlobally {
		b.SetEncoding(b.Encoding)
	}

	data, encErr := payloads.Marshal(payload, b.Encoding)
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
		Channel:  filter,
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

	log.Info("Publishing")
	pubErr := b.beacon.Publish(wrappedSend)
	if pubErr != nil {
		return pubErr
	}

	b.publishing = true
	b.hasStarted = true
	return nil

}

// StopBroadcast will stop the beacon broadcast.
func (b *BeaconClient) StopBroadcast(f string) error {
	b.beacon.Silence()
	b.publishing = false
	return nil
}

func (c *BeaconClient) SetConnectionDropHook(callback func() error) error {
	// no-op
	return nil
}