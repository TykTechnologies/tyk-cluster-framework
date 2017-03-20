package client

import (
	"testing"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"encoding/json"
)

func TestDefaultMessageHandler(t *testing.T) {
	mh := NewMessageHandler()
	var p Payload
	var err error
	rawMessage := testPayloadData{FullName:"Foo"}
	if p, err = NewPayload(rawMessage); err != nil {
		t.Fatal(err)
	}

	// This test assumes JSON encoding!
	p.Encode()
	asRaw, _ := json.Marshal(p)

	var p1 Payload
	if p1, err = mh.HandleRawMessage(asRaw, encoding.JSON); err != nil {
		t.Fatal(err)
	}

	var resp testPayloadData
	if err = p1.DecodeMessage(&resp); err != nil {
		t.Fatal(err)
	}

	if resp.FullName != "Foo" {
		t.Fatalf("Value incorrect: %v", resp.FullName)
	}
}