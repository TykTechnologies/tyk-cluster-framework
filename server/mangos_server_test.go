package server

import (
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
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
		if _, err = c.Subscribe("null", func(payload client.Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d)
			if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}
		}); err != nil {
			t.Fatal(err)
		}

		time.Sleep(1 * time.Second)

		p, _ := client.NewPayload("Hello")
		c.Publish("Test", p)

		time.Sleep(300 * time.Millisecond)

		if len(s.Connections()) == 0 {
			t.Fatal("Connections should be larger than 0!")
		}


		c.Stop()
	})

	// Test stop
	if err = s.Stop(); err != nil {
		t.Fatal(err)
	}

}
