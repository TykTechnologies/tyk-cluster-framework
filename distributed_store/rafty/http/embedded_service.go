package httpd

import (
	"errors"
	rafty_objects "github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/objects"
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/store"
	"gopkg.in/vmihailenco/msgpack.v2"
	"net/url"
	"strconv"
)

type forwardingCommand string

const (
	forward_create           forwardingCommand = "create"
	forward_get              forwardingCommand = "get"
	forward_update           forwardingCommand = "update"
	forward_delete           forwardingCommand = "delete"
	forward_add_to_set       forwardingCommand = "add_to_set"
	forward_lpush            forwardingCommand = "lpush"
	forward_lrem             forwardingCommand = "lrem"
	forward_zadd             forwardingCommand = "zadd"
	forward_zremrangebyscore forwardingCommand = "zremrangebyscore"
)

type EmbeddedService struct {
	storageAPI *StorageAPI
	TLS        bool
}

type ForwardNodeValue struct {
	rafty_objects.NodeValue
	LPush struct {
		Values []interface{}
	}
	LREM struct {
		Count int
		Value interface{}
	}
	ZADD struct {
		Score int64
		Value interface{}
	}
	ZREMRANGEBYSCORE struct {
		Min int64
		Max int64
	}
}

func NewEmbeddedService(useTLS bool, storageAPI *StorageAPI) *EmbeddedService {
	return &EmbeddedService{
		storageAPI: storageAPI,
		TLS:        useTLS,
	}
}

func (e *EmbeddedService) Leader() string {
	return e.storageAPI.store.Leader()
}

func (e *EmbeddedService) IsLeader() bool {
	return e.storageAPI.store.IsLeader()
}

func (e *EmbeddedService) AddToSet(key string, value []byte) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: string(value),
		Key:   key,
	}

	if !e.storageAPI.store.IsLeader() {
		return e.forwardCommand(key, forward_add_to_set, &ForwardNodeValue{NodeValue: *nodeData})
	}

	var err *ErrorResponse
	if _, err = e.storageAPI.AddToSet(key, value); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeySetAdded)
	returnData.Node = nodeData
	return returnData, nil
}

func (e *EmbeddedService) GetSet(k string) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "",
		Key:   k,
	}

	var err *ErrorResponse
	var value map[interface{}]interface{}
	if value, err = e.storageAPI.GetSet(k); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeySetRequested)
	returnData.Node = nodeData
	returnData.Meta = value
	return returnData, nil
}

func (e *EmbeddedService) LPush(key string, values ...interface{}) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "",
		Key:   key,
	}

	if !e.storageAPI.store.IsLeader() {
		f := ForwardNodeValue{NodeValue: *nodeData}
		f.LPush.Values = values
		return e.forwardCommand(key, forward_lpush, &f)
	}

	var err *ErrorResponse
	if err = e.storageAPI.LPush(key, values...); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyListPush)
	returnData.Node = nodeData
	return returnData, nil
}

func (e *EmbeddedService) LLen(key string) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "0",
		Key:   key,
	}

	var err *ErrorResponse
	var val int64
	if val, err = e.storageAPI.LLen(key); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyListLength)
	returnData.Node = nodeData
	returnData.Meta = val

	return returnData, nil
}

func (e *EmbeddedService) LRem(key string, count int, value interface{}) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "0",
		Key:   key,
	}

	if !e.storageAPI.store.IsLeader() {
		f := ForwardNodeValue{NodeValue: *nodeData}
		f.LREM.Count = count
		f.LREM.Value = value
		return e.forwardCommand(key, forward_lrem, &f)
	}

	var err *ErrorResponse
	if err = e.storageAPI.LRem(key, count, value); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyListRemove)
	returnData.Node = nodeData

	return returnData, nil
}

func (e *EmbeddedService) LRange(key string, from, to int) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "0",
		Key:   key,
	}

	var err *ErrorResponse
	var val []interface{}
	if val, err = e.storageAPI.LRange(key, from, to); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyListRange)
	returnData.Node = nodeData
	returnData.Meta = val

	return returnData, nil
}

func (e *EmbeddedService) ZAdd(key string, score int64, value interface{}) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "0",
		Key:   key,
	}

	if !e.storageAPI.store.IsLeader() {
		f := ForwardNodeValue{NodeValue: *nodeData}
		f.ZADD.Value = value
		f.ZADD.Score = score
		return e.forwardCommand(key, forward_zadd, &f)
	}

	var err *ErrorResponse
	if err = e.storageAPI.ZAdd(key, score, value); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyZSetAdd)
	returnData.Node = nodeData

	return returnData, nil
}

func (e *EmbeddedService) ZRangeByScore(key string, min, max int64) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "0",
		Key:   key,
	}

	var err *ErrorResponse
	var val []interface{}
	if val, err = e.storageAPI.ZRangeByScore(key, min, max); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyZSetRangeByScore)
	returnData.Node = nodeData
	returnData.Meta = val

	return returnData, nil
}

func (e *EmbeddedService) ZRemRangeByScore(key string, min int64, max int64) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   0,
		Value: "0",
		Key:   key,
	}

	if !e.storageAPI.store.IsLeader() {
		f := ForwardNodeValue{NodeValue: *nodeData}
		f.ZREMRANGEBYSCORE.Max = max
		f.ZREMRANGEBYSCORE.Max = min
		return e.forwardCommand(key, forward_zremrangebyscore, &f)
	}

	var err *ErrorResponse
	if err = e.storageAPI.ZRemRangeByScore(key, min, max); err != nil {
		return nil, err
	}

	returnData := NewKeyValueAPIObjectWithAction(ActionKeyZSetRemRangeByScore)
	returnData.Node = nodeData

	return returnData, nil
}

func (e *EmbeddedService) CreateKey(key string, value string, ttl int) (*KeyValueAPIObject, error) {
	nodeData := &rafty_objects.NodeValue{
		TTL:   ttl,
		Value: value,
		Key:   key,
	}

	if !e.storageAPI.store.IsLeader() {
		return e.forwardCommand(key, forward_create, &ForwardNodeValue{NodeValue: *nodeData})
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

func (e *EmbeddedService) UpdateKey(key, value string, ttl int) (*KeyValueAPIObject, error) {
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
		return e.forwardCommand(key, forward_update, &ForwardNodeValue{NodeValue: *nodeValue})
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
		return e.forwardCommand(key, forward_delete, nil)
	}
	if _, err := e.storageAPI.DeleteKey(key); err != nil {
		return nil, err
	}

	delResp := NewKeyValueAPIObjectWithAction(ActionKeyDeleted)
	delResp.Node.Key = "/" + key

	return delResp, nil
}

func (e *EmbeddedService) forwardCommand(key string, command forwardingCommand, value *ForwardNodeValue) (*KeyValueAPIObject, error) {
	trans := "http"
	if e.TLS {
		trans = "https"
	}

	apiHost := store.GetHttpAPIFromRaftURL(e.storageAPI.store.Leader())
	if apiHost == "" {
		return nil, errors.New("Leader unknown, can't forward")
	}

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
	case forward_add_to_set:
		return c.AddToSet(key, []byte(value.Value))
	case forward_lpush:
		return c.LPush(key, value.LPush.Values)
	case forward_lrem:
		return c.LRem(key, value.LREM.Count, value.LREM.Value)
	case forward_zadd:
		return c.ZAdd(key, value.ZADD.Score, value.ZADD.Value)
	case forward_zremrangebyscore:
		return c.ZRemRangeByScore(key, value.ZREMRANGEBYSCORE.Min, value.ZREMRANGEBYSCORE.Max)
	}

	return nil, errors.New("Command not recognised")
}
