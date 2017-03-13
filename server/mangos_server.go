package server

import (
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pub"
	"github.com/go-mangos/mangos/transport/tcp"
	"fmt"
	"errors"
)

type MangosServer struct {
	listening bool
	sock mangos.Socket
	conf *MangosServerConf
	encoding client.Encoding
}

type MangosServerConf struct {
	Encoding client.Encoding
	listenOn string
}

func newMangoConfig(listenOn string) *MangosServerConf {
	return &MangosServerConf{listenOn: listenOn, Encoding: client.JSON}
}

func (s *MangosServer) Init(config interface{}) error {
	s.conf = config.(*MangosServerConf)
	s.SetEncoding(s.conf.Encoding)

	return nil
}

func (s *MangosServer) startListening() error {
	var err error
	if s.sock, err = pub.NewSocket(); err != nil {
		return fmt.Errorf("can't get new pub socket: %s", err)
	}

	s.sock.AddTransport(tcp.NewTransport())
	if err = s.sock.Listen(s.conf.listenOn); err != nil {
		return fmt.Errorf("can't listen on pub socket: %s", err.Error())
	}

	return nil
}

func (s *MangosServer) Listen() error {
	if !s.listening {
		err := s.startListening()
		if err != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf-mangos",
			}).Fatal(err)
		}
		s.listening = true
	}

	return nil
}

func (s *MangosServer) EnableBroadcast(enabled bool) {
	// no op
}

func (s *MangosServer) SetEncoding(enc client.Encoding) error {
	s.encoding = enc
	return nil
}

func (s *MangosServer) Stop() error {
	if s.listening {
		return s.sock.Close()
	}
	return errors.New("Already stopped")
}
