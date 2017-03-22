package client

import (
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"testing"
)

func TestUnmarshal(t *testing.T) {
	rawMessage := testPayloadData{
		FullName: "foo",
	}

	p, err := NewPayload(rawMessage)
	if err != nil {
		t.Fatal(err)
	}

	v, err := Marshal(p, encoding.JSON)

	if err != nil {
		t.Fatal(err)
	}

	tm, _ := NewPayload(struct{}{})
	err = Unmarshal(tm, v, encoding.JSON)

	if err != nil {
		t.Fatal(err)
	}

	tv := testPayloadData{}
	if err = tm.DecodeMessage(&tv); err != nil {
		t.Fatal(err)
	}

	if tv.FullName != rawMessage.FullName {
		t.Fatal("Incorrect message data")
	}
}

