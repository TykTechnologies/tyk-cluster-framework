package verifier

import (
	"testing"
	"encoding/json"
)

type testPayloadData struct {
	FullName string
}

func TestHMAC256(t *testing.T) {
	var v Verifier
	var err error
	if v, err = NewVerifier("HMAC256", "123456789"); err != nil {
		t.Fatal(err)
	}

	rawMessage := testPayloadData{
		FullName: "foo",
	}


	var val interface{}
	val, err = json.Marshal(rawMessage)

	if err != nil {
		t.Fatal(err)
	}

	var computedSig string
	computedSig, err = v.Sign(val.([]byte))
	if err != nil {
		t.Fatal(err)
	}

	if computedSig == "" {
		t.Fatal("Signature is empty!")
	}

	// This is the computed val so should be ok
	if err = v.Verify(val.([]byte), computedSig); err != nil {
		// This should be ok, so no error
		t.Fatal(err)
	}

	// Test with a broken value
	if err = v.Verify(val.([]byte), "PrincessCOnsuellaBananaHammock"); err == nil {
		t.Fatal("Verification must fail!")
	}
}
