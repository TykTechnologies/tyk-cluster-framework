package main

import (
	"github.com/TykTechnologies/tyk-cluster-framework/server"
	"log"
	"time"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

func main() {
	var s server.Server
	var sErr error
	// Must be a specific IP, otherwise client-side publishing will fail
	if s, sErr = server.NewServer("mangos://127.0.0.1:9100", encoding.JSON); sErr != nil {
		log.Fatal(sErr)
	}

	log.Println("Listening for 600 seconds")
	s.Listen()

	time.Sleep(600 * time.Second)
}
