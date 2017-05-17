package rafty

// Rafty is the raft k/v implementation, heavily influenced by
// the hraftd server (https://github.com/otoolep/hraftd) written by Philip O'Toole
// and based on Hashicorps Raft implementation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/http"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/store"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"time"

	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"path/filepath"
)

var log = logger.GetLogger()
var logPrefix string = "tcf.rafty"

type MasterConfigPayload struct {
	HttpServerAddr string
}

// StartServer will start a rafty server based on it's configuration. Most of this is handled by the distributed store parent library.
func StartServer(JoinAddress string, raftyConfig *Config, killChan chan os.Signal, broadcastWith client.Client, serviceChan chan *httpd.EmbeddedService) {
	log.Info("Log level: ", os.Getenv("TYK_LOGLEVEL"))
	if raftyConfig == nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Warning("No raft configuration found, using defaults")
		raftyConfig = raftyConfig
	}

	// Ensure Raft storage exists.
	raftDir := raftyConfig.RaftDir
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

	var masterConfigChan = make(chan MasterConfigPayload)
	if broadcastWith != nil {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Info("Starting master boradcaster")
		go startBroadcast(broadcastWith, s, &MasterConfigPayload{HttpServerAddr: raftyConfig.HttpServerAddr})
		startListeningForMasterChange(broadcastWith, masterConfigChan)

		if raftyConfig.RunInSingleServerMode == false {
			select {
			case masterConfig := <-masterConfigChan:
				raftyConfig.RunInSingleServerMode = false
				JoinAddress = masterConfig.HttpServerAddr
				log.WithFields(logrus.Fields{
					"prefix": logPrefix,
				}).Info("Got leader. Joining: ", JoinAddress)
				break

			case <-time.After(time.Second * 5):
				// Timeout reached, lets re-bootstrap
				raftyConfig.RunInSingleServerMode = true

				if raftyConfig.ResetPeersOnLoad {
					thisPath := filepath.Join(s.RaftDir, "peers.json")
					store.ResetPeersJSON(thisPath, raftyConfig.RaftServerAddress)
				}

				log.WithFields(logrus.Fields{
					"prefix": logPrefix,
				}).Info("No leader found, starting and waiting for set peers")
			}
		}

		go masterListener(masterConfigChan, raftyConfig)
	}

	// Only allow bootstrap if no join is specified
	log.Info("Running in single server mode: ", raftyConfig.RunInSingleServerMode)

	if err := s.Open(raftyConfig.RunInSingleServerMode); err != nil {
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
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Info("Sending join request")
		if err := join(JoinAddress, raftyConfig.RaftServerAddress, raftyConfig.TLSConfig != nil); err != nil {
			log.WithFields(logrus.Fields{
				"prefix": logPrefix,
			}).Fatalf("Failed to join node at %s: %v", JoinAddress, err)
		}

	}

	// Return a pointer to the storage API
	serviceChan <- h.EmbeddedAPI

	signal.Notify(killChan, os.Interrupt)
	<-killChan
	log.WithFields(logrus.Fields{
		"prefix": logPrefix,
	}).Info("Raft server exiting")

	if !s.IsLeader() {
		log.WithFields(logrus.Fields{
			"prefix": logPrefix,
		}).Info("Leaving cluster")
		leaveErr := leave(s.Leader(), raftyConfig.RaftServerAddress, raftyConfig.TLSConfig != nil)
		if leaveErr != nil {
			log.WithFields(logrus.Fields{
				"prefix": logPrefix,
			}).Error("Raft server tried to leave, error: ", leaveErr)
		}
		return
	}

	log.WithFields(logrus.Fields{
		"prefix": logPrefix,
	}).Info("Leader leaving cluster")
	s.RemovePeer(raftyConfig.RaftServerAddress)
}

func masterListener(inBoundChan chan MasterConfigPayload, raftyConfig *Config) {
	lastWrite := time.Now().Unix()
	for {
		masterConfig := <-inBoundChan
		if masterConfig.HttpServerAddr != raftyConfig.HttpServerAddr {
			// Stop flooding the log
			if (time.Now().Unix() - lastWrite) > int64(10) {
				log.WithFields(logrus.Fields{
					"prefix": logPrefix,
				}).Info("Leader is: ", masterConfig.HttpServerAddr)
				lastWrite = time.Now().Unix()
			}

		}

	}

}

func startBroadcast(msgClient client.Client, s *store.Store, raftyConfig *MasterConfigPayload) {
	var isPublishing bool
	for {
		if !isPublishing {
			if s.IsLeader() {
				thisPayload, pErr := payloads.NewPayload(*raftyConfig)

				if pErr != nil {
					log.WithFields(logrus.Fields{
						"prefix": logPrefix,
					}).Fatal(pErr)
				}

				log.WithFields(logrus.Fields{
					"prefix": "tcf-exp",
				}).Debug("Sending Broadcast: %v", raftyConfig.HttpServerAddr)
				if bErr := msgClient.Broadcast("tcf.cluster.distributed_store.leader", thisPayload, 1); bErr != nil {
					log.WithFields(logrus.Fields{
						"prefix": logPrefix,
					}).Fatal(bErr)
				}
				isPublishing = true
			} else {
				msgClient.StopBroadcast("tcf.cluster.distributed_store.leader")
				isPublishing = false
			}
		}
		time.Sleep(time.Microsecond * 100)
	}
}

func startListeningForMasterChange(msgClient client.Client, configChan chan MasterConfigPayload) {
	msgClient.Subscribe("tcf.cluster.distributed_store.leader", func(payload payloads.Payload) {
		var d MasterConfigPayload
		decErr := payload.DecodeMessage(&d)
		var skip bool
		if decErr != nil {
			log.WithFields(logrus.Fields{
				"prefix": logPrefix,
			}).Error(decErr)
			skip = true
		}

		if !skip {
			log.WithFields(logrus.Fields{
				"prefix": "tcf-exp",
			}).Debugf("RECEIVED: %v", d)

			configChan <- d
		}
	})
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

	apiAddr := store.GetHttpAPIFromRaftURL(leaderAddr)

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
