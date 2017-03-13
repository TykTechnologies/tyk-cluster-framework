package server

import (
	"testing"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"time"
	"fmt"
)

type testPayloadData struct {
	FullName string
}

func TestMangosServer(t *testing.T) {
	var s Server
	var err error
	if s, err = NewServer("mangos://0.0.0.0:9100", client.JSON); err != nil {
		t.Fatal(err)
	}

	s.Listen()

	// Test pub/sub
	t.Run("Client Side Publish", func(t *testing.T){
		var err error
		var c client.Client

		ch := "tcf.test.mangos-server.client-publish"
		msg := "Tyk Cluster Framework: Client"
		resultChan := make(chan testPayloadData)

		if c, err = client.NewClient("mangos://127.0.0.1:9100", client.JSON); err != nil {
			t.Fatal(err)
		}

		// Connect
		if err = c.Connect(); err != nil {
			t.Fatal(err)
		}

		// Subscribe to some stuff
		var subChan chan string
		if subChan, err = c.Subscribe(ch, func(payload client.Payload) {
			var d testPayloadData
			err := payload.DecodeMessage(&d); if err != nil {
				t.Fatalf("Decode payload failed: %v", err)
			}

			resultChan <- d
		}); err != nil {
			t.Fatal("err")
		}

		var dp client.Payload
		if dp, err = client.NewPayload(testPayloadData{msg}); err != nil {
			t.Fatal(err)
		}

		select {
		case s := <- subChan:
			if s != ch {
				t.Fatal("Incorrect subscribe channel returned!")
			}
			fmt.Printf("Subscription confirmed: %v\n", s)
		case <-time.After(time.Millisecond * 100):
			t.Fatalf("Channel wait timed out")
		}

		// This is ugly, but mangos handles connect in the background, so we need to wait :-/
		time.Sleep(time.Millisecond * 100)
		fmt.Println("Publishing")
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
		close(resultChan)
	})

	// Test stop
	if err = s.Stop(); err != nil {
		t.Fatal(err)
	}

}