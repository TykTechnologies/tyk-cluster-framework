package client

import (
	"encoding/json"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"testing"
)

func TestHandleRawMessage(t *testing.T) {
	ch := ClientHandler{}
	rawMessage := testPayloadData{
		FullName: "foo",
	}

	pl, err := NewPayload(rawMessage)
	if err != nil {
		t.Fatal(err)
	}

	asByte, err := json.Marshal(pl)
	if err != nil {
		t.Fatal(err)
	}

	ph := func(p Payload) {
		var d testPayloadData
		decErr := p.DecodeMessage(&d)
		if decErr != nil {
			t.Fatalf("Decode payload failed: %v was: %v", decErr, p)
		}

		if d.FullName != "foo" {
			t.Fatalf("Value incorrect: %v\n", d)
		}
	}

	if err := ch.HandleRawMessage(asByte, ph, encoding.JSON); err != nil {
		t.Fatal(err)
	}
}
