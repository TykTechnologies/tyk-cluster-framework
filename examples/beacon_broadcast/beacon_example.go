package main

import (
	"time"
	"fmt"
	"log"

	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

type testPayloadData struct {
	FullName string
}

const FILTER string = "tcf.names"
var tcfClient client.Client

func main() {
	// Create a client
	var err error
	if tcfClient, err = client.NewClient("beacon://localhost:9898?interval=1", encoding.JSON); tErr != nil {
		log.Fatal(err)
	}

	// Connect
	if err = tcfClient.Connect(); err != nil {
		log.Fatal(err)
	}

	// Set up a subscription and payload handler
	tcfClient.Subscribe(FILTER, func(payload client.Payload) {
		var d testPayloadData
		if decErr := payload.DecodeMessage(&d); decErr != nil {
			log.Fatalf("Decode payload failed: %v, was: %v", decErr, payload)
		}

		fmt.Printf("RECEIVED: %v\n", d.FullName)
	})

	// Because all of this is non-blocking, we need to block here for some input
	startBroadcast("Test message")
	time.Sleep(time.Second * 10)

	// Stop broadcasting
	if err = tcfClient.StopBroadcast(FILTER); err != nil {
		log.Fatal(err)
	}

	// Let it finish
	time.Sleep(time.Second * 5)

}

func startBroadcast(Payload string) {
	var p client.Payload
	var err error

	m := testPayloadData{FullName: Payload}
	p, err = client.NewPayload(m); if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Broadcasting: %v\n", m)

	if err = tcfClient.Broadcast(FILTER, p, 1); err != nil {
		log.Fatal(err)
	}
}

