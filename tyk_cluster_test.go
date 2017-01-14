package tcf

//
//import (
//	"testing"
//	"log"
//	"time"
//)
//
//func TestUsage(t *testing.T) {
//	// Create a server
//	s, _ := NewServer("dummy://0.0.0.0:4344", JSON)
//	s.EnableBroadcast(true)
//	s.Listen()
//
//	// Create another server
//	s2, _ := NewServer("dummy://0.0.0.0:4345", JSON)
//	s2.EnableBroadcast(true)
//	s2.Listen()
//	// Output: "Discovered more tcf servers, bridging"
//
//	// Create a client
//	c, _ := NewClient("dummy://auto", JSON)
//	connectErr := c.Connect()
//	if connectErr != nil {
//		log.Fatal(connectErr)
//	}
//
//	c.Subscribe("filter1", func(payload Payload) {})
//	c.Subscribe("filter2", func(payload Payload) {})
//
//	// Servers can publish
//	s.Publish("filter1", DefaultPayload{
//		Message: "foo",
//		Sig: "",
//		Time: time.Now().Unix(),
//	})
//
//	// Clients can publish too
//	c.Publish("filter2", DefaultPayload{
//		Message: "bar",
//		Sig: "",
//		Time: time.Now().Unix(),
//	})
//}
