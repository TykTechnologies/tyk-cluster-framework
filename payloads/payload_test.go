package payloads

import (
	"testing"
	"time"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
)

func TestDefaultPayload(t *testing.T) {
	var p Payload
	var err error
	testMsg := testPayloadData{FullName:"Foo"}
	if p, err = NewPayload(testMsg); err != nil {
		t.Fatal(err)
	}

	t.Run("Timestamp", func(t *testing.T){
		if p.TimeStamp().After(time.Now()) {
			t.Fatalf("Timestamp is in future: %v\n", p.TimeStamp())
		}
	})

	t.Run("Encode and Decode", func(t *testing.T){
		if err = p.Encode(); err != nil {
			t.Fatal(err)
		}

		var resp testPayloadData
		if err = p.DecodeMessage(&resp); err != nil {
			t.Fatal(err)
		}

		if resp.FullName != "Foo" {
			t.Fatalf("Value is wrong: %v\n", resp.FullName)
		}
	})

	t.Run("Set Encoding", func (t *testing.T) {
		p.SetEncoding(encoding.JSON)
		if p.(*DefaultPayload).Encoding != encoding.JSON {
			t.Fatal("Encoding is incorrect: %v\n", p.(*DefaultPayload).Encoding)
		}
	})

	t.Run("Verify (encoded HMAC default)", func (t *testing.T) {
		p.Encode()
		if err = p.Verify(); err != nil {
			t.Fatalf("Verification should not fail when message is encoded: %v", err)
		}
	})
}
