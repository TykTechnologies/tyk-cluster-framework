package httpd

import (
	rafty_objects "github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/objects"
	"gopkg.in/vmihailenco/msgpack.v2"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/store"
	"net/url"
	"strconv"
	"errors"
)

type forwardingCommand string
const (
	forward_create forwardingCommand = "create"
	forward_get forwardingCommand = "get"
	forward_update forwardingCommand = "update"
	forward_delete forwardingCommand = "delete"
)

type EmbeddedService struct{
	storageAPI *StorageAPI
	TLS bool
}

func NewEmbeddedService(useTLS bool, storageAPI *StorageAPI) *EmbeddedService {
	return &EmbeddedService{
		storageAPI: storageAPI,
		TLS: useTLS,
	}
}

func (e *EmbeddedService) CreateKey(key string, value string, ttl int) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL: ttl,
		Value: value,
		Key: key,
	}

	if !e.storageAPI.store.IsLeader() {
		return e.forwardCommand(key, forward_create, nodeData)
	}

	var n *rafty_objects.NodeValue
	var err *ErrorResponse
	if n, err = e.storageAPI.SetKey(key, nodeData, false); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyCreated)
	returnData.Node = n
	return returnData, nil
}

func (e *EmbeddedService) UpdateKey(key, value string, ttl int) (*KeyValueAPIObject, error)  {
	// Get the existing value
	v, errResp := e.storageAPI.getKeyFromStore(key)
	if errResp != nil {
		return nil, NewErrorResponse("/"+key, "Not found")
	}

	// Decode it
	nodeValue := &rafty_objects.NodeValue{}
	mDecErr := msgpack.Unmarshal(v, nodeValue)
	if mDecErr != nil {
		return nil, NewErrorResponse("/"+key, "Key marshalling failed: "+mDecErr.Error())
	}


	// Set expiry value if it has changed
	if nodeValue.TTL != ttl {
		nodeValue.CalculateExpiry()
		nodeValue.TTL = ttl
	}

	// update actual value
	nodeValue.Value = value

	// Write data to the store
	if !e.storageAPI.store.IsLeader() {
		e.forwardCommand(key, forward_update, nodeValue)
	}
	var err *ErrorResponse
	var newNodeValue *rafty_objects.NodeValue
	if newNodeValue, err = e.storageAPI.SetKey(key, nodeValue, true); err != nil {
		return nil, NewErrorResponse("/"+key, "Could not write to store: "+err.ErrorCode.Reason)
	}

	// Return ok
	returnData := NewKeyValueAPIObjectWithAction(ActionKeyModified)
	returnData.Node = newNodeValue

	return returnData, nil
}

func (e *EmbeddedService) GetKey(key string) (*KeyValueAPIObject, error) {
	// Get the existing value
	returnValue, errResp := e.storageAPI.GetKey(key, false)
	if errResp != nil {
		return nil, errResp
	}

	return returnValue, nil
}

func (e *EmbeddedService) DeleteKey(key string) (*KeyValueAPIObject, error) {

	if !e.storageAPI.store.IsLeader() {
		e.forwardCommand(key, forward_delete, nil)
	}
	if _, err := e.storageAPI.DeleteKey(key); err != nil {
		return nil, err
	}

	delResp := NewKeyValueAPIObjectWithAction(ActionKeyDeleted)
	delResp.Node.Key = "/" + key

	return delResp, nil
}

func (e *EmbeddedService) forwardCommand(key string, command forwardingCommand, value *rafty_objects.NodeValue) (*KeyValueAPIObject, error) {
	trans := "http"
	if e.TLS {
		trans = "https"
	}

	apiHost := store.GetHttpAPIFromRaftURL(e.storageAPI.store.Leader())
	targetAddr := trans + "://" + apiHost

	_, urlErr := url.Parse(targetAddr)
	if urlErr != nil {
		log.Error("Failed to generate leader HTTP address: ", urlErr, " was: ", targetAddr)
		return nil, NewErrorResponse("/"+key, "Failed to forward to leader")
	}

	c := NewRaftyClient(targetAddr)

	switch command {
	case forward_get:
		return c.GetKey(key)
	case forward_update:
		return c.UpdateKey(key, value.Value, strconv.Itoa(value.TTL))
	case forward_create:
		return c.CreateKey(key, value.Value, strconv.Itoa(value.TTL))
	case forward_delete:
		return c.DeleteKey(key)
	}

	return nil, errors.New("Command not recognised")
}