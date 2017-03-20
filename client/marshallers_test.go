package client

import (
	"encoding/json"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"testing"
	"time"
)

func TestMarshal(t *testing.T) {
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

	var x map[string]interface{}
	err = json.Unmarshal(v.([]byte), &x)
	if err != nil {
		t.Fatal(err)
	}

	if x["Message"] != `{"FullName":"foo"}` {
		t.Fatalf("Message wrong: %v\n", x["Message"])
	}

	if x["Encoding"] != `application/json` {
		t.Fatalf("Encoding wrong: %v\n", x["Encoding"])
	}

	if int64(x["Time"].(float64)) > time.Now().Unix() {
		t.Fatalf("Timestamp is in the future: %v\n", x["Time"])
	}
}
