package tcf

import (
	"fmt"
	"github.com/TykTechnologies/logrus"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/http"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"github.com/nu7hatch/gouuid"
	"net"
	"os"
)

var log *logrus.Logger = logger.GetLogger()
var DistributedStores map[string]chan os.Signal = make(map[string]chan os.Signal)

// Distributed Store provides a distributed key/value store using the raft protocol (see the rafty sub-package), it
// provides a full HTTP and embedded API for interaction with the store
type DistributedStore struct {
	config     *rafty.Config
	GetAdvertiseAddr func() (*net.TCPAddr, error)
	StorageAPI *httpd.EmbeddedService
	serverID   string
}

// NewDistributedStore will create a new DS object ready to start serving
func NewDistributedStore(config *rafty.Config) (*DistributedStore, error) {
	d := DistributedStore{}

	d.config = &rafty.Config{
		HttpServerAddr:    rafty.DefaultHTTPAddr,
		RaftServerAddress: rafty.DefaultRaftAddr,
		JoinTimeout:       60,
		RaftDir:           "raft",
	}

	if config != nil {
		// Change hostnames to IP addresses
		//httpAddr, err := net.ResolveTCPAddr("tcp", config.HA)
		//if err != nil {
		//	return nil, err
		//}
		//
		//raftAddr, err := net.ResolveTCPAddr("tcp", config.RaftServerAddress)
		//if err != nil {
		//	return nil, err
		//}
		//
		//config.HA = httpAddr.String()
		//config.RaftServerAddress = raftAddr.String()

		d.config = config
	}

	return &d, nil
}

// Start will start the store, set joinAddress to force a connection to an existing cluster, and set broadcastWith to a
// tcf Client to enable auto-discovery of a cluster when bootstrapping.
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

// Stop will shut down the store and leave the cluster. it;s recommend to stop the leader last.
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
