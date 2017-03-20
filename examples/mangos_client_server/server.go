package main

import (
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/server"
	"log"
	"time"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

func main() {
	var s server.Server
	var sErr error
	if s, sErr = server.NewServer("mangos://0.0.0.0:9100", encoding.JSON); sErr != nil {
		log.Fatal(sErr)
	}

	log.Println("Listening for 600 seconds")
	s.Listen()

	time.Sleep(600 * time.Second)
}
