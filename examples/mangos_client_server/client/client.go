package main

import (
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"

	"fmt"
	"time"
	"log"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)


type testPayloadData struct {
	FullName string
}

const CHANNAME string = "tcf.names"
var tcfClient client.Client

func main() {
	var err error
	if tcfClient, err = client.NewClient("mangos://tcf-test:9100", encoding.JSON); err != nil {
		log.Fatal(err)
	}

	// Connect
	if err = tcfClient.Connect(); err != nil {
		log.Fatal(err)
	}

	// Subscribe to some stuff
	s, subErr := tcfClient.Subscribe(CHANNAME, func(payload payloads.Payload) {
		var d testPayloadData
		if decErr := payload.DecodeMessage(&d);  decErr != nil {
			log.Fatalf("Decode payload failed: %v was: %v\n", decErr, payload)
		}

		fmt.Printf("RECEIVED: %v\n", d.FullName)
	})

	if subErr != nil {
		log.Fatal(subErr)
	}

	select {
	case v := <-s:
		if v != CHANNAME {
			log.Fatal("Incorrect subscribe channel returned!")
		}
	case <-time.After(time.Millisecond * 500):
		log.Fatalf("Channel wait timed out")
	}

	go sendTestMessages("Foo")


	// The above does not block, so lets wait so we can get all the messages
	time.Sleep(5 * time.Second)
}

func sendTestMessages(Payload string) {
	for _, v := range []string{"1", "2", "3", "4", "5"} {
		m := testPayloadData{FullName: Payload + ": " + v}
		var p payloads.Payload
		var pErr error

		if p, pErr = payloads.NewPayload(m); pErr != nil {
			log.Fatal(pErr)
		}

		if pErr = tcfClient.Publish(CHANNAME, p); pErr != nil {
			log.Fatal(pErr)
		}
		log.Printf("Sent: %v\n", m)

		time.Sleep(time.Second * 1)
	}
}