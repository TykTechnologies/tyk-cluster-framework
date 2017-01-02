package rafty

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/TykTechnologies/tyk-cluster-framework/rafty/http"
	"github.com/TykTechnologies/tyk-cluster-framework/rafty/store"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"github.com/TykTechnologies/logrus"
)

var log = logger.GetLogger()
var logPrefix string = "tcf.rafty"

func StartServer(JoinAddress string, raftyConfig *Config) {
	if raftyConfig == nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Warning("No raft configuration found, using defaults")
		raftyConfig = &raftyConfig
	}

	// Ensure Raft storage exists.
	raftDir  := raftyConfig.RaftDir
	if raftDir == "" {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Error("No Raft storage directory specified")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	s := store.New()
	s.RaftDir = raftyConfig.RaftDir
	s.RaftBind = raftyConfig.RaftServerAddress
	if err := s.Open(JoinAddress == ""); err != nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Fatal("Failed to open store: ", err)
	}

	h := httpd.New(raftyConfig.HttpServerAddr, s)
	if err := h.Start(); err != nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Fatalf("Failed to start HTTP service: %v", err)
	}

	// If join was specified, make the join request.
	if JoinAddress != "" {
		if err := join(JoinAddress, raftyConfig.RaftServerAddress); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": logPrefix,
			}).Fatalf("Failed to join node at %s: %v", JoinAddress, err)
		}
	}

	log.WithFields(logrus.Fields{
		"prefix": logPrefix,
	}).Info("Raft server started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.WithFields(logrus.Fields{
		"prefix": logPrefix,
	}).Info("Raft server exiting")
}

func join(joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}
	resp, err := http.Post(
		fmt.Sprintf("http://%s/join", joinAddr),
		"application-type/json",
		bytes.NewReader(b))

	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
