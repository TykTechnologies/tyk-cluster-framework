package httpd

import (
	"fmt"
	rafty_objects "github.com/TykTechnologies/tyk-cluster-framework/rafty/objects"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type ActionType string

const (
	ActionKeyCreated   ActionType = "created"
	ActionKeyModified  ActionType = "modified"
	ActionKeyDeleted   ActionType = "deleted"
	ActionKeyRequested ActionType = "requested"
)

type KeyValueAPIObject struct {
	Action ActionType               `json:"action"`
	Node   *rafty_objects.NodeValue `json:"node"`
}

// NewKeyValueAPIObject creates a new object for use in the APi
func NewKeyValueAPIObject() *KeyValueAPIObject {
	return &KeyValueAPIObject{
		Action: ActionKeyCreated,
		Node:   &rafty_objects.NodeValue{},
	}
}

func NewKeyValueAPIObjectWithAction(action ActionType) *KeyValueAPIObject {
	return &KeyValueAPIObject{
		Action: action,
		Node:   &rafty_objects.NodeValue{},
	}
}

// NewKeyValueAPIObjectFromMsgPack will generate a new API object from a msgpack payload from the store
func NewKeyValueAPIObjectFromMsgPack(payload []byte) (*KeyValueAPIObject, error) {
	thisKV := NewKeyValueAPIObjectWithAction(ActionKeyRequested)
	var err error
	err = msgpack.Unmarshal(payload, &thisKV.Node)
	return thisKV, err
}

type ErrorCode struct {
	Code   int    `json:"code"`
	Reason string `json:"reason"`
}

var (
	RAFTErrorNotFound        ErrorCode = ErrorCode{100, "Key not found"}
	RAFTErrorWithApplication ErrorCode = ErrorCode{101, "Application error"}
	RAFTErrorKeyExists       ErrorCode = ErrorCode{102, "Key Exists"}
)

type ErrorResponse struct {
	Cause    string      `json:"cause"`
	Error    ErrorCode   `json:"error"`
	MetaData interface{} `json:"metaData,omitempty"`
}

func (e *ErrorResponse) String() string {
	return fmt.Sprintf("API error from: %s reason: %s (%v) metadata: %v", e.Cause, e.Error.Reason, e.Error.Code, e.MetaData)
}

func NewErrorResponse(cause string, metadata interface{}) *ErrorResponse {
	return &ErrorResponse{
		Cause:    cause,
		Error:    RAFTErrorWithApplication,
		MetaData: metadata,
	}
}

func NewErrorNotFound(cause string) *ErrorResponse {
	return &ErrorResponse{
		Cause: cause,
		Error: RAFTErrorNotFound,
	}
}
