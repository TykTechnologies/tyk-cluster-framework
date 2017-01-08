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
	"strings"
	"strconv"
)

var log = logger.GetLogger()
var logPrefix string = "tcf.rafty"

var JOINED_STATE bool

func StartServer(JoinAddress string, raftyConfig *Config, killChan chan os.Signal) {
	log.Info("Log level: ", os.Getenv("TYK_LOGLEVEL"))
	if raftyConfig == nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Warning("No raft configuration found, using defaults")
		raftyConfig = raftyConfig
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

	// We always allow single node mode
	if err := s.Open(true); err != nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Fatal("Failed to open store: ", err)
	}

	h := httpd.New(raftyConfig.HttpServerAddr, s, raftyConfig.TLSConfig)
	if err := h.Start(); err != nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Fatalf("Failed to start HTTP service: %v", err)
	}

	// If join was specified, make the join request.
	if JoinAddress != "" {
		if err := join(JoinAddress, raftyConfig.RaftServerAddress, raftyConfig.TLSConfig != nil); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": logPrefix,
			}).Fatalf("Failed to join node at %s: %v", JoinAddress, err)
		}

		JOINED_STATE = true
	}

	log.WithFields(logrus.Fields{
		"prefix": logPrefix,
	}).Info("Raft server started successfully")

	signal.Notify(killChan, os.Interrupt)
	<-killChan
	log.WithFields(logrus.Fields{
		"prefix": logPrefix,
	}).Info("Raft server exiting")

	if !s.IsLeader() {
		log.Info("LEAVING!")
		leaveErr := leave(s.Leader(), raftyConfig.RaftServerAddress, raftyConfig.TLSConfig != nil)
		if leaveErr != nil {
			log.WithFields(logrus.Fields{
				"prefix": logPrefix,
			}).Error("Raft server tried to leave, error: ", leaveErr)
		}
		return
	}

	s.RemovePeer(raftyConfig.RaftServerAddress)
}

func join(joinAddr, raftAddr string, secure bool) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}

	trans := "http"
	if secure {
		trans = "https"
	}
	resp, err := http.Post(
		fmt.Sprintf(trans+"://%s/join", joinAddr),
		"application-type/json",
		bytes.NewReader(b))

	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func leave(leaderAddr, raftAddr string, secure bool) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}

	trans := "http"
	if secure {
		trans = "https"
	}

	urlParts := strings.Split(leaderAddr, ":")
	var host, portStr string
	if len(urlParts) > 1 {
		host = urlParts[0]
		portStr = urlParts[1]
	} else {
		host = leaderAddr
	}

	asInt, intErr := strconv.Atoi(portStr)
	if intErr != nil {
		log.Fatal(intErr, "was: ", portStr)
	}
	apiAddr := host + ":" + strconv.Itoa(asInt - 100)

	resp, err := http.Post(
		fmt.Sprintf(trans+"://%s/remove", apiAddr),
		"application-type/json",
		bytes.NewReader(b))

	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
