package client

import (
	"testing"
	"time"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"os"
)


// If Redis is remote, can be set with an env variable: TCF_TEST_REDIS,
// otherwise localhost assumed
func TestRedisClient(t *testing.T) {
	redisServer := os.Getenv("TCF_TEST_REDIS")
	if redisServer == "" {
		redisServer = "localhost:9100"
	}
	cs := "redis://"+redisServer

	// Test pub/sub
	t.Run("Client Side Publish", func(t *testing.T){
		var err error
		var c Client

		ch := "tcf.test.redis-server.client-publish"
		msg := "Tyk Cluster Framework: Client"
		resultChan := make(chan testPayloadData)

		if c, err = NewClient(cs, encoding.JSON); err != nil {
			t.Fatal(err)
		}

		// Connect
		if err = c.Connect(); err != nil {
			t.Fatal(err)
		}

		// Subscribe to some stuff
		var subChan chan string
		if subChan, err = c.Subscribe(ch, func(payload Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d); if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}

			resultChan <- d
		}); err != nil {
			t.Fatal("err")
		}

		var dp Payload
		if dp, err = NewPayload(testPayloadData{msg}); err != nil {
			t.Fatal(err)
		}

		select {
		case s := <- subChan:
			if s != ch {
				t.Fatal("Incorrect subscribe channel returned!")
			}
		case <-time.After(time.Second * 3):
			t.Fatalf("Channel wait timed out")
		}

		if err = c.Publish(ch, dp); err != nil {
			t.Fatal(err)
		}

		select {
		case v := <- resultChan:
			if v.FullName != msg {
				t.Fatalf("Unexpected return value: %v", v)
			}
		case <-time.After(time.Millisecond * 100):
			t.Fatalf("Timed out")
		}

		c.Stop()
	})

	// Test multiple subs with a single client
	t.Run("Multiple Client Side Subs", func(t *testing.T){
		var err error
		var c1 Client

		ch1 := "tcf.test.redis-server.client-sub1"
		ch2 := "tcf.test.redis-server.client-sub2"

		resultChan1 := make(chan testPayloadData)
		resultChan2 := make(chan testPayloadData)

		if c1, err = NewClient(cs, encoding.JSON); err != nil {
			t.Fatal(err)
		}

		// Connect
		if err = c1.Connect(); err != nil {
			t.Fatal(err)
		}

		// Subscribe to some stuff
		var subChan chan string
		if subChan, err = c1.Subscribe(ch1, func(payload Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d); if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}

			resultChan1 <- d
		}); err != nil {
			t.Fatal("err")
		}

		// Subscribe to some stuff
		if subChan, err = c1.Subscribe(ch2, func(payload Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d); if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}

			resultChan2 <- d
		}); err != nil {
			t.Fatal("err")
		}

		var dpChan1, dpChan2 Payload
		ch1Msg := "Channel 1"
		ch2Msg := "Channel 2"
		if dpChan1, err = NewPayload(testPayloadData{ch1Msg}); err != nil {
			t.Fatal(err)
		}
		if dpChan2, err = NewPayload(testPayloadData{ch2Msg}); err != nil {
			t.Fatal(err)
		}

		select {
		case s := <- subChan:
			if s != ch1 && s != ch2 {
				t.Fatalf("Incorrect subscribe channel returned: %v", s)
			}
		case <-time.After(time.Second * 3):
			t.Fatalf("Channel wait timed out")
		}

		select {
		case s := <- subChan:
			if s != ch1 && s != ch2 {
				t.Fatalf("Incorrect subscribe channel returned: %v", s)
			}
		case <-time.After(time.Second * 3):
			t.Fatalf("Channel wait timed out")
		}

		if err = c1.Publish(ch1, dpChan1); err != nil {
			t.Fatal(err)
		}
		if err = c1.Publish(ch2, dpChan2); err != nil {
			t.Fatal(err)
		}

		// Inverted result channels here so we can test async
		select {
		case v := <- resultChan2:
			if v.FullName != ch2Msg {
				t.Fatalf("Unexpected return value: %v", v)
			}
		case <-time.After(time.Millisecond * 100):
			t.Fatalf("Chan 2 timed out")
		}

		select {
		case v := <- resultChan1:
			if v.FullName != ch1Msg {
				t.Fatalf("Unexpected return value: %v", v)
			}
		case <-time.After(time.Millisecond * 100):
			t.Fatalf("Chan 1 timed out")
		}

		c1.Stop()
	})

	// Test multiple subscribes with one client, but ignore the subs notification channel
	// because it might be unused for brevity
	t.Run("Multiple Client Side Subs, but ignore sub channel", func(t *testing.T){
		var err error
		var c1 Client

		ch1 := "tcf.test.redis-server.client-sub1"
		ch2 := "tcf.test.redis-server.client-sub2"

		resultChan3 := make(chan testPayloadData)
		resultChan4 := make(chan testPayloadData)

		if c1, err = NewClient(cs, encoding.JSON); err != nil {
			t.Fatal(err)
		}

		// Connect
		if err = c1.Connect(); err != nil {
			t.Fatal(err)
		}

		// Subscribe to some stuff
		if _, err = c1.Subscribe(ch1, func(payload Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d); if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}

			resultChan3 <- d
		}); err != nil {
			t.Fatal("err")
		}

		// Subscribe to some stuff
		if _, err = c1.Subscribe(ch2, func(payload Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d); if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}

			resultChan4 <- d
		}); err != nil {
			t.Fatal("err")
		}

		var dpChan1, dpChan2 Payload
		ch1Msg := "Channel 1"
		ch2Msg := "Channel 2"
		if dpChan1, err = NewPayload(testPayloadData{ch1Msg}); err != nil {
			t.Fatal(err)
		}
		if dpChan2, err = NewPayload(testPayloadData{ch2Msg}); err != nil {
			t.Fatal(err)
		}

		if err = c1.Publish(ch1, dpChan1); err != nil {
			t.Fatal(err)
		}
		if err = c1.Publish(ch2, dpChan2); err != nil {
			t.Fatal(err)
		}

		// Inverted result channels here so we can test async
		// TImings must be longer because we have no subs confirmation
		select {
		case v := <- resultChan4:
			if v.FullName != ch2Msg {
				t.Fatalf("Unexpected return value: %v", v)
			}
		case <-time.After(time.Second * 5):
			t.Fatalf("Chan 2 timed out")
		}

		select {
		case v := <- resultChan3:
			if v.FullName != ch1Msg {
				t.Fatalf("Unexpected return value: %v", v)
			}
		case <-time.After(time.Second * 5):
			t.Fatalf("Chan 1 timed out")
		}

		c1.Stop()
	})

}
