package tcf

import (
	"github.com/TykTechnologies/tyk-cluster-framework/rafty"
	"os"
	"github.com/nu7hatch/gouuid"
	"github.com/TykTechnologies/logrus"
	"fmt"
)

var DistributedStores map[string]chan os.Signal = make(map[string]chan os.Signal)

type DistributedStore struct {
	config *rafty.Config
	serverID string
}

func NewDistributedStore(config *rafty.Config) (*DistributedStore, error) {
	d := DistributedStore{}

	d.config = &rafty.Config {
		HttpServerAddr: rafty.DefaultHTTPAddr,
		RaftServerAddress: rafty.DefaultRaftAddr,
		JoinTimeout: 60,
		RaftDir: "raft",
	}

	if config != nil {
		d.config = config
	}

	return &d, nil
}

func (d *DistributedStore) Start(joinAddress string) {
	u, _ := uuid.NewV4()
	serverID := u.String()
	d.serverID = serverID
	termChan := make(chan os.Signal, 1)
	DistributedStores[serverID] = termChan
	go rafty.StartServer(joinAddress, d.config, termChan)
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


