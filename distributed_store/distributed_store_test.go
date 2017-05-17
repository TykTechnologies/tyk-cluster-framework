package tcf

import (
	"encoding/json"
	"fmt"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/http"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/levigross/grequests"
	"math/rand"
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
	os.RemoveAll("raft-test1")
	os.RemoveAll("raft-test2")
	os.RemoveAll("raft-test3")

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

	t.Run("Is leader set correctly", func(t *testing.T) {
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

	t.Run("Test create", func(t *testing.T) {
		var v *httpd.KeyValueAPIObject
		var err error

		v, err = ds1.StorageAPI.CreateKey("create-test-1", "foo", 999)

		if err != nil {
			t.Fatal(err)
		}

		if v.Action != httpd.ActionKeyCreated {
			t.Fatal("Wrong action returned")
		}

		if v.Node.Value != "foo" {
			t.Fatal("Incorrect value saved")
		}
	})
	t.Run("Test read", func(t *testing.T) {
		_, err := ds1.StorageAPI.CreateKey("create-test-2", "foo", 999)
		if err != nil {
			t.Fatal(err)
		}

		g, gErr := ds1.StorageAPI.GetKey("create-test-2")
		if gErr != nil {
			t.Fatal(err)
		}

		if g.Node.Value != "foo" {
			t.Fatal("Incorrect value returned by GET")
		}
	})
	t.Run("Test update", func(t *testing.T) {
		k := "update-test-1"
		uv := "bar"
		_, err := ds1.StorageAPI.CreateKey(k, "foo", 999)
		if err != nil {
			t.Fatal(err)
		}

		_, gErr := ds1.StorageAPI.GetKey(k)
		if gErr != nil {
			t.Fatal(err)
		}

		v2, uErr := ds1.StorageAPI.UpdateKey(k, uv, 666)
		if uErr != nil {
			t.Fatal(uErr)
		}

		if v2.Node.Value != uv {
			t.Fatalf("Return value from update should be updated, instead is %v", v2.Node.Value)
		}

		g2, g2Err := ds1.StorageAPI.GetKey(k)
		if g2Err != nil {
			t.Fatal(err)
		}

		if g2.Node.TTL != 666 {
			t.Fatal("Incorrect modified TTL")
		}

		if g2.Node.Value != uv {
			t.Fatalf("Value not updated! Expected %v, got: %v", uv, g2.Node.Value)
		}
	})
	t.Run("Test delete", func(t *testing.T) {
		k := "delete-test-1"
		_, err := ds1.StorageAPI.CreateKey(k, "foo", 999)
		if err != nil {
			t.Fatal(err)
		}

		_, gErr := ds1.StorageAPI.GetKey(k)
		if gErr != nil {
			t.Fatal(err)
		}

		if _, dErr := ds1.StorageAPI.DeleteKey(k); dErr != nil {
			t.Fatal(dErr)
		}

		_, g2Err := ds1.StorageAPI.GetKey(k)
		if g2Err == nil {
			t.Fatal("Value was not deleted!")
		}

	})

	t.Run("Test Set Ops", func(t *testing.T) {
		k := "sets-test-1"

		// Sets
		_, err := ds1.StorageAPI.AddToSet(k, []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = ds1.StorageAPI.AddToSet(k, []byte("bar"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = ds1.StorageAPI.AddToSet(k, []byte("baz"))
		if err != nil {
			t.Fatal(err)
		}

		// This should not be added
		_, err = ds1.StorageAPI.AddToSet(k, []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		ret, gErr := ds1.StorageAPI.GetSet(k)
		if gErr != nil {
			t.Fatal(err)
		}

		if len(ret.Meta.(map[interface{}]interface{})) != 3 {
			t.Fatalf("Set should have 3 memeberts, has %v", len(ret.Meta.(map[interface{}]interface{})))
		}
	})

	t.Run("Test List Ops", func(t *testing.T) {
		k := "list-test-1"

		// Lists
		_, err := ds1.StorageAPI.LPush(k, "foo", "bar", "baz", "foo")
		if err != nil {
			t.Fatal(err)
		}

		lVal, lErr := ds1.StorageAPI.LLen(k)
		if lErr != nil {
			t.Fatal(lErr)
		}

		if lVal.Meta.(int64) != 4 {
			t.Fatalf("Lists has wrong number of members, expected 4 got: %v", lVal.Meta.(int64))
		}

		_, err = ds1.StorageAPI.LRem(k, 2, "foo")
		if err != nil {
			t.Fatal(err)
		}

		// Get all
		lRange, lrErr := ds1.StorageAPI.LRange(k, 0, -1)
		if lrErr != nil {
			t.Fatal(lrErr)
		}

		r := lRange.Meta.([]interface{})
		if len(r) != 2 {
			t.Fatalf("After LREM, wrong number of elements in list, expected 2, got: %v", len(r))
		}

		ds1.StorageAPI.LPush(k, "fee", "fie", "foe", "fum")
		lRange, lrErr = ds1.StorageAPI.LRange(k, 0, -1)
		if lrErr != nil {
			t.Fatal(lrErr)
		}

		r = lRange.Meta.([]interface{})
		if len(r) != 6 {
			t.Fatalf("Wrong number of elements in range list, expected 6, got: %v", len(r))
		}

		lRange, lrErr = ds1.StorageAPI.LRange(k, 0, -3)
		if lrErr != nil {
			t.Fatal(lrErr)
		}

		r = lRange.Meta.([]interface{})
		if len(r) != 4 {
			t.Fatalf("Wrong number of elements in range list, expected 4, got: %v", len(r))
		}

		lRange, lrErr = ds1.StorageAPI.LRange(k, -3, -1)
		if lrErr != nil {
			t.Fatal(lrErr)
		}

		r = lRange.Meta.([]interface{})
		if len(r) != 3 {
			t.Fatalf("Wrong number of elements in range list, expected 3, got: %v", len(r))
		}
	})

	t.Run("Test Sorted Set Ops", func(t *testing.T) {
		// Add entries:
		var err error
		k := "zset-test-1"

		if _, err = ds1.StorageAPI.ZAdd(k, 1, "foo"); err != nil {
			t.Fatal(err)
		}
		if _, err = ds1.StorageAPI.ZAdd(k, 2, "bar"); err != nil {
			t.Fatal(err)
		}
		if _, err = ds1.StorageAPI.ZAdd(k, 3, "baz"); err != nil {
			t.Fatal(err)
		}
		if _, err = ds1.StorageAPI.ZAdd(k, 4, "bim"); err != nil {
			t.Fatal(err)
		}

		var r1 *httpd.KeyValueAPIObject
		if r1, err = ds1.StorageAPI.ZRangeByScore(k, 3, 5); err != nil {
			t.Fatal(err)
		}

		if len(r1.Meta.([]interface{})) != 2 {
			t.Fatalf("ZRangeByScore expected 2, got %v", len(r1.Meta.([]interface{})))
		}

		if _, err = ds1.StorageAPI.ZAdd(k, 5, "boo"); err != nil {
			t.Fatal(err)
		}
		if _, err = ds1.StorageAPI.ZAdd(k, 5, "borp"); err != nil {
			t.Fatal(err)
		}

		var r2 *httpd.KeyValueAPIObject
		if r2, err = ds1.StorageAPI.ZRangeByScore(k, 3, 5); err != nil {
			t.Fatal(err)
		}

		if len(r2.Meta.([]interface{})) != 4 {
			t.Fatalf("ZRangeByScore expected 4, got %v", len(r2.Meta.([]interface{})))
		}

		if _, err = ds1.StorageAPI.ZRemRangeByScore(k, 3, 5); err != nil {
			t.Fatal(err)
		}

		var r3 *httpd.KeyValueAPIObject
		if r3, err = ds1.StorageAPI.ZRangeByScore(k, 0, 100); err != nil {
			t.Fatal(err)
		}

		if len(r3.Meta.([]interface{})) != 2 {
			t.Fatalf("ZRangeByScore expected 2, got %v", len(r3.Meta.([]interface{})))
		}

		for i, item := range r3.Meta.([]interface{}) {
			if i == 0 && item.(string) != "foo" {
				t.Fatalf("Expected 'foo', got '%v'", item)
			}

			if i == 1 && item.(string) != "bar" {
				t.Fatalf("Expected 'bar', got '%v'", item)
			}
		}
	})

	// Tear-down
	ds1.Stop()
	ds2.Stop()
	ds3.Stop()
}

func getBenchClient() (client.Client, error) {

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

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

type tdType struct {
	Payload string
	N       string
}

func BenchmarkDistributedStoreRaftyClient(b *testing.B) {
	// Kill all the leftover data
	os.RemoveAll("raft-test1")
	os.RemoveAll("raft-test2")
	os.RemoveAll("raft-test3")

	c1, err := getBenchClient()
	if err != nil {
		b.Fatal(err)
	}
	c2, err := getBenchClient()
	if err != nil {
		b.Fatal(err)
	}
	c3, err := getBenchClient()
	if err != nil {
		b.Fatal(err)
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

	rc := httpd.NewRaftyClient("http://127.0.0.1:11100")

	rc2 := httpd.NewRaftyClient("http://127.0.0.1:11101")

	// Create a test key
	rc.CreateKey("benchtest-read", tdType{Payload: RandStringRunes(100), N: "100"}, "0")
	b.Run("READ SPEED MASTER", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rc.GetKey("benchtest-read")
		}
	})

	b.Run("READ SPEED SLAVE", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rc2.GetKey("benchtest-read")
		}
	})

	writeBenchmarks := []tdType{
		{Payload: RandStringRunes(100), N: "100"},
	}

	for _, v := range writeBenchmarks {
		// Writes
		b.Run(fmt.Sprintf("WRITE SPEED: %v", v.N), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				rc.CreateKey("benchtest-"+v.N+RandStringRunes(10), v, "0")
			}
		})
	}

	// Tear-down
	ds1.Stop()
	ds2.Stop()
	ds3.Stop()
}

func BenchmarkDistributedStoreEmbeddedClient(b *testing.B) {
	// Kill all the leftover data
	os.RemoveAll("raft-test1")
	os.RemoveAll("raft-test2")
	os.RemoveAll("raft-test3")

	c1, err := getBenchClient()
	if err != nil {
		b.Fatal(err)
	}
	c2, err := getBenchClient()
	if err != nil {
		b.Fatal(err)
	}
	c3, err := getBenchClient()
	if err != nil {
		b.Fatal(err)
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

	asJSON, _ := json.Marshal(tdType{Payload: RandStringRunes(100), N: "100"})

	// Create a test key
	ds1.StorageAPI.CreateKey("benchtest-read", string(asJSON), 0)
	b.Run("READ SPEED MASTER", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ds1.StorageAPI.GetKey("benchtest-read")
		}
	})

	b.Run("READ SPEED SLAVE", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ds2.StorageAPI.GetKey("benchtest-read")
		}
	})

	writeBenchmarks := []string{
		string(asJSON),
	}

	for _, v := range writeBenchmarks {
		// Writes
		b.Run(fmt.Sprint("WRITE SPEED"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ds1.StorageAPI.CreateKey("benchtest-"+RandStringRunes(10), v, 0)
			}
		})
	}

	// Tear-down
	ds1.Stop()
	ds2.Stop()
	ds3.Stop()
}
