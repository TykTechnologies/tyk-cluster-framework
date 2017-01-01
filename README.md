# Tyk Cluster Framework

This package is a wrapper around pub/sub systems to allow for a simple way to swap out underlying mechanisms (e.g. redis / zmq / amqp).

The use case is to be able to swap out redis for another messaging broker such as ZMQ or AMQP without breaking the wider use of the underlying functiuonality by the dependent system.

USage example:

```
package main

import (
	"github.com/TykTechnologies/tyk-cluster-framework"
	"log"
	"strconv"
	"time"
)

type testPayloadData struct {
	FullName string
}

const CHANNAME string = "tcf.names"
var tcfClient tcf.Client

func main() {
	// Create a client
	var tErr error
	tcfClient, tErr = tcf.NewClient("redis://redis.host.somewhere:6379", tcf.JSON)
	if tErr != nil {
		log.Fatal(tErr)
	}

	// Connect
	connectErr := tcfClient.Connect()
	if connectErr != nil {
		log.Fatal(connectErr)
	}

	// Subscribe to some stuff
	tcfClient.Subscribe(CHANNAME, func(payload tcf.Payload) {
		var d testPayloadData
		decErr := payload.DecodeMessage(&d)
		if decErr != nil {
			log.Fatal(decErr)
		}

		log.Printf("RECEIVED: %v \n", d.FullName)
	})

	// Lets send some test messages
	go SendTestMessages()

	// Bewcause all of this is non-blocking, we need to block here for some input
	time.Sleep(time.Second * 10)
}

// Sends an incrementing counter indefinitely.
func SendTestMessages() {
	cnt := 0
	for {
		thisMessage := testPayloadData{FullName: strconv.Itoa(cnt)}
		thisPayload, pErr := tcf.NewPayload(thisMessage)

		if pErr != nil {
			log.Fatal(pErr)
		}

		log.Printf("SENDING: %v \n", thisMessage)
		tcfClient.Publish(CHANNAME, thisPayload)
		time.Sleep(time.Second * 1)
		cnt += 1
	}
}
```

