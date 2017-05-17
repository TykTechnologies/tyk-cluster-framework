package bus

import (
	"errors"
	"fmt"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"strings"

	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tykcommon-logger"
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/bus"
	"github.com/go-mangos/mangos/transport/tcp"
	"github.com/satori/go.uuid"
)

type Bus struct {
	client.ClientHandler
	conn_str        string
	sock            mangos.Socket
	me              string
	enc             encoding.Encoding
	id              string
	rawMode         bool
	payloadHandlers map[string]client.PayloadHandler
	onRawMessage    func([]byte) error
	stopChan        chan struct{}
}

var log = logger.GetLogger()

func NewBus(conn_str, me string, rawMode bool, enc encoding.Encoding) (*Bus, error) {
	return &Bus{
		conn_str:        conn_str,
		me:              me,
		enc:             enc,
		rawMode:         rawMode,
		id:              uuid.NewV4().String(),
		payloadHandlers: make(map[string]client.PayloadHandler),
		stopChan:        make(chan struct{}),
	}, nil
}

func (b *Bus) Connect() error {
	fragment := strings.Split(b.conn_str, "://")
	if len(fragment) < 2 {
		return errors.New("Connection string must start with transport (e.g.: tcp://server:port)")
	}

	hosts := strings.Split(fragment[1], ",")
	if len(hosts) < 2 {
		return errors.New("Connection string must have at least two or more hosts")
	}

	var err error
	for _, h := range hosts {
		if h == b.me || h == "" {
			continue
		}

		fixedHost := fmt.Sprintf("tcp://%v", h)

		if err = b.sock.Dial(fixedHost); err != nil {
			return fmt.Errorf("socket.Dial: %s", err.Error())
		}
	}

	return nil
}

func (b *Bus) GetID() string {
	return b.id
}

func (b *Bus) Subscribe(topic string, handler client.PayloadHandler) {
	b.payloadHandlers[topic] = handler
}

func (b *Bus) SetOnRawMsg(handler func([]byte) error) {
	b.onRawMessage = handler
}

func (b *Bus) handleRawPayload(msg []byte) error {
	if b.onRawMessage != nil {
		return b.onRawMessage(msg)
	}

	return nil
}

func (b *Bus) handlePayload(msg []byte) error {
	if b.rawMode {
		return b.handleRawPayload(msg)
	}

	pl, err := b.GetPayload(msg, b.enc)
	if err != nil {
		return err
	}

	handler, found := b.payloadHandlers[pl.GetTopic()]
	if found {
		handler(pl)
	}

	return nil
}

func (b *Bus) Listen() error {
	var err error
	var msg []byte

	if b.sock, err = bus.NewSocket(); err != nil {
		return fmt.Errorf("bus.NewSocket: %s", err)
	}

	b.sock.AddTransport(tcp.NewTransport())
	listenOn := fmt.Sprintf("tcp://%s", b.me)
	if err = b.sock.Listen(listenOn); err != nil {
		return fmt.Errorf("sock.Listen: %s", err.Error())
	}

	for {
		if msg, err = b.sock.Recv(); err != nil {
			return fmt.Errorf("sock.Recv: %s", err.Error())
		}

		select {
		case <-b.stopChan:
			break
		default:
			// nothing
		}

		// TODO: might want this as a pool
		go b.handlePayload(msg)
	}
}

func (b *Bus) Stop() error {
	b.stopChan <- struct{}{}
	return b.sock.Close()
}

func (b *Bus) Send(topic string, payload payloads.Payload) error {
	if payload == nil {
		return nil
	}

	payload.SetTopic(topic)
	if payload.From() == "" {
		payload.SetFrom(b.GetID())
	}

	data, encErr := payloads.Marshal(payload, b.enc)
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

	if len(encodedPayload) == 0 {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.MangosClient",
		}).Error("No data to send, not sending")
		return nil
	}

	if err := b.sock.Send(encodedPayload); err != nil {
		return fmt.Errorf("sock.Send: %s", err.Error())
	}

	return nil
}

func (b *Bus) SendRaw(value []byte) error {
	if err := b.sock.Send(value); err != nil {
		return fmt.Errorf("sock.Send: %s", err.Error())
	}

	return nil
}
