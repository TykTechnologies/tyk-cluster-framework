package server

import (
	"testing"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

type testPayloadData struct {
	FullName string
}

func TestMangosServer(t *testing.T) {
	var s Server
	var err error
	if s, err = NewServer("mangos://0.0.0.0:9100", encoding.JSON); err != nil {
		t.Fatal(err)
	}

	s.Listen()

	// Test pub/sub
	t.Run("Client Side Connect", func(t *testing.T){
		var err error
		var c client.Client

		if c, err = client.NewClient("mangos://127.0.0.1:9100", encoding.JSON); err != nil {
			t.Fatal(err)
		}

		// Connect
		if err = c.Connect(); err != nil {
			t.Fatal(err)
		}

		c.Stop()
	})

	// Test stop
	if err = s.Stop(); err != nil {
		t.Fatal(err)
	}

}
