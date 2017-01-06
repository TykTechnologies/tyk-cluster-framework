package httpd

import (
	"time"
	rafty_objects "github.com/TykTechnologies/tyk-cluster-framework/rafty/objects"

)

// StorageAPI exposes the raw store getters and setters
type StorageAPI struct{
	store Store
}

func (s *StorageAPI) GetKey(k string) (*KeyValueAPIObject, *ErrorResponse) {
	// Get the existing value
	v, errResp := s.getKeyFromStore(k)
	if errResp != nil {
		return nil, errResp
	}

	// Decode it
	returnValue, err := NewKeyValueAPIObjectFromMsgPack(v)
	if err != nil {
		return nil, NewErrorResponse("/"+k, "Key marshalling failed: " + err.Error())

	}

	if time.Now().After(returnValue.Node.Expiration) {
		return nil, NewErrorNotFound("/"+k)
	}

	return returnValue, nil
}

func (s *StorageAPI) SetKey(k string, value *rafty_objects.NodeValue) (*rafty_objects.NodeValue, *ErrorResponse) {
	// Don't allow overwriting unless expired
	existingValue, errResp := s.getKeyFromStore(k)
	asNode, checkErr := NewKeyValueAPIObjectFromMsgPack(existingValue)

	var allowOverwrite bool
	if checkErr == nil {
		if time.Now().After(asNode.Node.Expiration) {
			allowOverwrite = true
		}
	}

	if errResp == nil && allowOverwrite == false {
		keyExistsErr := &ErrorResponse{Cause: "/"+k, Error: RAFTErrorKeyExists}
		return nil, keyExistsErr
	}

	value.Key = k

	// Set expiry value
	value.CalculateExpiry()
	value.Created = time.Now().Unix()
	toStore, encErr := value.EncodeForStorage()

	if encErr != nil {
		return nil, NewErrorResponse("/"+k, "Could not encode payload for store: "+encErr.Error())
	}

	// Write data to the store
	if err := s.store.Set(k, toStore); err != nil {
		return nil, NewErrorResponse("/"+k, "Could not write to store: "+err.Error())
	}

	return value, nil
}

func (s *StorageAPI) DeleteKey(k string) (*KeyValueAPIObject, *ErrorResponse) {
	if err := s.store.Delete(k); err != nil {
		return nil, NewErrorResponse("/"+k, "Delete failed: " + err.Error())
	}

	return nil, nil
}

func (s *StorageAPI) getKeyFromStore(k string) ([]byte, *ErrorResponse) {
	if k == "" {
		return nil, NewErrorResponse("/"+k, "Key cannot be empty")
	}

	v, err := s.store.Get(k)
	if err != nil {
		thisErr := NewErrorNotFound("/"+k)
		thisErr.MetaData = err.Error()
		return nil, thisErr
	}

	if v == nil {
		thisErr := NewErrorNotFound("/"+k)
		return nil, thisErr
	}

	return v, nil
}