package tcf

import (
	"fmt"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/http"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"github.com/nu7hatch/gouuid"
	"os"
)

var log *logrus.Logger = logger.GetLogger()
var DistributedStores map[string]chan os.Signal = make(map[string]chan os.Signal)

type DistributedStore struct {
	config     *rafty.Config
	StorageAPI *httpd.EmbeddedService
	serverID   string
}

func NewDistributedStore(config *rafty.Config) (*DistributedStore, error) {
	d := DistributedStore{}

	d.config = &rafty.Config{
		HttpServerAddr:    rafty.DefaultHTTPAddr,
		RaftServerAddress: rafty.DefaultRaftAddr,
		JoinTimeout:       60,
		RaftDir:           "raft",
	}

	if config != nil {
		d.config = config
	}

	return &d, nil
}

func (d *DistributedStore) Start(joinAddress string, broadcastWith client.Client) {
	u, _ := uuid.NewV4()
	serverID := u.String()
	d.serverID = serverID
	termChan := make(chan os.Signal, 1)
	DistributedStores[serverID] = termChan
	storageChan := make(chan *httpd.EmbeddedService)
	go rafty.StartServer(joinAddress, d.config, termChan, broadcastWith, storageChan)

	// We want to be able to use the server functions directly without calling the http API
	d.StorageAPI = <-storageChan

	log.WithFields(logrus.Fields{
		"prefix": "distributed_store",
	}).Info("Distrubuted storage engine started: ", serverID)
}

func (d *DistributedStore) Stop() error {
	killChan, found := DistributedStores[d.serverID]
	if !found {
		return fmt.Errorf("Could not find server ID to stop: %v", d.serverID)
	}

	log.WithFields(logrus.Fields{
		"prefix": "distributed_store",
	}).Info("Stopping server: ", d.serverID)
	killChan <- os.Interrupt

	return nil
}
