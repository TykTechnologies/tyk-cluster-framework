package server

import (
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"testing"
	"time"
)

type testPayloadData struct {
	FullName string
}

func TestMangosServer(t *testing.T) {
	var s Server
	var err error
	if s, err = NewServer("mangos://127.0.0.1:9100", encoding.JSON); err != nil {
		t.Fatal(err)
	}

	if err = s.Listen(); err != nil {
		t.Fatal(err)
	}

	// Test connect
	t.Run("Client Side Connect", func(t *testing.T) {
		var err error
		var c client.Client

		if c, err = client.NewClient("mangos://127.0.0.1:9100", encoding.JSON); err != nil {
			t.Fatal(err)
		}

		// Connect
		if err = c.Connect(); err != nil {
			t.Fatal(err)
		}

		// A client must subscribe first to initiate a conn:
		// Subscribe to some stuff
		if _, err = c.Subscribe("null", func(payload payloads.Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d)
			if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}
		}); err != nil {
			t.Fatal(err)
		}

		time.Sleep(1 * time.Second)

		p, _ := payloads.NewPayload("Hello")
		c.Publish("Test", p)

		time.Sleep(300 * time.Millisecond)

		if len(s.Connections()) == 0 {
			t.Fatal("Connections should be larger than 0!")
		}

		c.Stop()
	})

	t.Run("Server Side Publish", func(t *testing.T) {
		var err error
		var c client.Client
		resultChan := make(chan testPayloadData)
		ch := "tcf.test.mangos-server.server-publish"
		msg := "Tyk Cluster Framework: Server"

		if c, err = client.NewClient("mangos://127.0.0.1:9100", encoding.JSON); err != nil {
			t.Fatal(err)
		}

		// Connect
		if err = c.Connect(); err != nil {
			t.Fatal(err)
		}

		// A client must subscribe first to initiate a conn:
		// Subscribe to some stuff
		var subChan chan string
		if subChan, err = c.Subscribe(ch, func(payload payloads.Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d)
			if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}

			resultChan <- d
		}); err != nil {
			t.Fatal(err)
		}

		var dp payloads.Payload
		if dp, err = payloads.NewPayload(testPayloadData{msg}); err != nil {
			t.Fatal(err)
		}

		select {
		case s := <-subChan:
			if s != ch {
				t.Fatal("Incorrect subscribe channel returned!")
			}
		case <-time.After(time.Millisecond * 500):
			t.Fatalf("Channel wait timed out")
		}

		time.Sleep(1 * time.Second)

		// This is ugly, but mangos handles connect in the background, so we need to wait :-/
		if err = s.Publish(ch, dp); err != nil {
			t.Fatal(err)
		}

		select {
		case v := <-resultChan:
			if v.FullName != msg {
				t.Fatalf("Unexpected return value: %v", v)
			}
		case <-time.After(time.Second * 5):
			t.Fatalf("Timed out")
		}

		c.Stop()
		time.Sleep(1 * time.Second)
		close(resultChan)
	})

	// Test stop
	if err = s.Stop(); err != nil {
		t.Fatal(err)
	}

}
