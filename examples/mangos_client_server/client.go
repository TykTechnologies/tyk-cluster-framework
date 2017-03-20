package main

import (
	"github.com/TykTechnologies/tyk-cluster-framework/client"

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
	if tcfClient, err = client.NewClient("mangos://127.0.0.1:9100", encoding.JSON); err != nil {
		log.Fatal(err)
	}

	// Connect
	if err = tcfClient.Connect(); err != nil {
		log.Fatal(err)
	}

	// Subscribe to some stuff
	tcfClient.Subscribe(CHANNAME, func(payload client.Payload) {
		var d testPayloadData
		if decErr := payload.DecodeMessage(&d);  decErr != nil {
			log.Fatalf("Decode payload failed: %v was: %v\n", decErr, payload)
		}

		fmt.Printf("RECEIVED: %v\n", d.FullName)
	})

	go sendTestMessages("Foo")

	// The above does not block, so lets wait so we can get all the messages
	time.Sleep(10 * time.Second)
}

func sendTestMessages(Payload string) {
	for _, v := range []string{"1", "2", "3", "4", "5"} {
		m := testPayloadData{FullName: Payload + ": " + v}
		var p client.Payload
		var pErr error

		if p, pErr = client.NewPayload(m); pErr != nil {
			log.Fatal(pErr)
		}

		if pErr = tcfClient.Publish(CHANNAME, p); pErr != nil {
			log.Fatal(pErr)
		}
		log.Printf("Sent: %v\n", m)

		time.Sleep(time.Second * 1)
	}
}