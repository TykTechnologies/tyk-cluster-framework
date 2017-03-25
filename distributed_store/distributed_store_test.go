package tcf

import (
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/http"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/levigross/grequests"
	"os"
	"testing"
	"time"
)

func getClient() (client.Client, error) {

	redisServer := os.Getenv("TCF_TEST_REDIS")
	if redisServer == "" {
		redisServer = "localhost:6379"
	}
	cs := "redis://" + redisServer

	c, err := client.NewClient(cs, encoding.JSON)
	if err != nil {
		return nil, err
	}

	// Connect
	connectErr := c.Connect()
	if connectErr != nil {
		panic(connectErr)
	}

	return c, nil
}

func TestDistributedStore(t *testing.T) {
	// Kill all the leftover data
	os.RemoveAll("./raft-test1")
	os.RemoveAll("./raft-test2")
	os.RemoveAll("./raft-test3")

	c1, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	c2, err := getClient()
	if err != nil {
		t.Fatal(err)
	}
	c3, err := getClient()
	if err != nil {
		t.Fatal(err)
	}

	raft1 := &rafty.Config{
		HttpServerAddr:        "127.0.0.1:11100",
		RaftServerAddress:     "127.0.0.1:11200",
		RaftDir:               "./raft-test1",
		RunInSingleServerMode: false,
		ResetPeersOnLoad:      true,
	}

	raft2 := &rafty.Config{
		HttpServerAddr:        "127.0.0.1:11101",
		RaftServerAddress:     "127.0.0.1:11201",
		RaftDir:               "./raft-test2",
		RunInSingleServerMode: false,
		ResetPeersOnLoad:      true,
	}

	raft3 := &rafty.Config{
		HttpServerAddr:        "127.0.0.1:11102",
		RaftServerAddress:     "127.0.0.1:11202",
		RaftDir:               "./raft-test3",
		RunInSingleServerMode: false,
		ResetPeersOnLoad:      true,
	}

	ds1, err := NewDistributedStore(raft1)
	ds2, err := NewDistributedStore(raft2)
	ds3, err := NewDistributedStore(raft3)

	ds1.Start("", c1)

	// Lets wait for the first instance to kick off so we have a master
	time.Sleep(time.Second * 10)
	ds2.Start("", c2)
	ds3.Start("", c3)
	time.Sleep(time.Second * 10)

	t.Run("Is Leader Set Correctly", func(t *testing.T) {
		resp, err := grequests.Get("http://127.0.0.1:11100/leader", nil)
		if err != nil {
			t.Fatal(err)
		}

		v := httpd.LeaderResponse{}
		err = resp.JSON(&v)
		if err != nil {
			t.Fatal(err)
		}

		if v.IsLeader != true {
			t.Fatalf("Leader not set correctly, got: %v", v.LeaderIs)
		}

		if v.LeaderIs != "127.0.0.1:11200" {
			t.Fatalf("Leader address not set correctly, got: %v", v.LeaderIs)
		}
	})

	// Tear-down
	ds1.Stop()
	ds2.Stop()
	ds3.Stop()
}
