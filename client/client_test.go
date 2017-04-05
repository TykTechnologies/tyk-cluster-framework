package client

import (
	"fmt"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"runtime"
	"strings"
	"testing"
)

func TestNewClient(t *testing.T) {
	ctypes := []string{
		"redis://foo:bar@localhost:6379/",
		"beacon://foo:bar@localhost:54321/",
		"mangos://foo:bar@localhost:54321/",
	}

	invalidCtypes := []string{
		"foo:bar@localhost:54321/",
		"dummy://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443/",
		"beacon://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443/",
		"beacon://localhost",
		"beacon://localhost:abc",
	}

	// Valid connection strings for back-ends
	for _, cs := range ctypes {
		if strings.Contains(cs, "beacon") && runtime.GOOS == "windows" {
			// Beacon doesn't work on windows
			fmt.Println("Skipping beacon test on windows")
		} else {
			t.Run("Valid c-string: "+cs, func(t *testing.T) {
				var err error
				if _, err = NewClient(cs, encoding.JSON); err != nil {
					t.Fatal(err)
				}
			})
		}
	}

	// Invalid CS strings for error handling
	for _, cs := range invalidCtypes {
		t.Run("Invalid c-string: "+cs, func(t *testing.T) {
			var err error
			if _, err = NewClient(cs, encoding.JSON); err == nil {
				t.Fatalf("%v Should have failed, but did not.\n", cs)
			}
		})
	}
}
