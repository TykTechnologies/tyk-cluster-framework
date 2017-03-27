package httpd

import (
	"testing"
	"fmt"
)

func returnsError() error {
	return NewErrorResponse("Foo", "metadata here")
}
func TestErrorResponse_Error(t *testing.T) {
	e := returnsError()
	fmt.Println(e.Error())

	if e == nil {
		t.Fatal("Error should not be nil")
	}
}
