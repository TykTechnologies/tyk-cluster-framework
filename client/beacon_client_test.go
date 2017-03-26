package client

import (
	"testing"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"time"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
)

func TestBeaconClient(t *testing.T) {
	// Beacon is only for broadcast so tests are a little different
	var b Client
	var err error
	resultChan := make(chan testPayloadData)

	if b, err = NewClient("beacon://0.0.0.0:9999?interval=1", encoding.JSON); err != nil {
		t.Fatal(err)
	}

	ch := "tcftestbeacon"
	chMsg := "Channel 1"
	var pl payloads.Payload
	if pl, err = payloads.NewPayload(testPayloadData{chMsg}); err != nil {
		t.Fatal(err)
	}

	if err := b.Broadcast(ch, pl, 1); err != nil {
		t.Fatal(err)
	}


	if _, err = b.Subscribe(ch, func(payload payloads.Payload) {
		var d testPayloadData
		err := payload.DecodeMessage(&d)
		if err != nil {
			t.Fatalf("Decode payload failed: %v", err)
		}

		resultChan <- d
	}); err != nil {
		t.Fatal(err)
	}

	loopCnt := 0
	msgCnt := 0
	for loopCnt = 0; loopCnt <= 3; loopCnt++ {
		select {
		case v := <-resultChan:
			if v.FullName != chMsg {
				t.Fatalf("Unexpected return value: %v", v)
			}
			msgCnt += 1
		case <-time.After(time.Second * 2):
			// fall through
		}
	}

	if msgCnt == 0 {
		t.Fatal("Received no messages")
	}

	if msgCnt < 3 {
		t.Fatalf("Received less than 3 messages, got: %v", msgCnt)
	}


	b.Stop()


}
