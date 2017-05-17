package httpd

import (
	rafty_objects "github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/objects"
	"github.com/foize/go.fifo"
	"time"

	"encoding/json"
	"github.com/TykTechnologies/logrus"
	"sync"
)

const (
	TTLSNAPSHOT_KEY = "TCF_TTL_SNAPHOT"
)

type SnapshotStatus int

const (
	StatusSnapshotFound     SnapshotStatus = 1
	StatusSnapshotNotFound  SnapshotStatus = 2
	StatusSnapshotNotLeader SnapshotStatus = 3
)

type qSnapShot struct {
	qmu           sync.Mutex
	queueSnapshot map[string]ttlIndexElement
}

func (q *qSnapShot) GetSnapshot() map[string]ttlIndexElement {
	return q.queueSnapshot
}

func (q *qSnapShot) SetElement(key string, value ttlIndexElement) {
	q.qmu.Lock()
	q.queueSnapshot[key] = value
	q.qmu.Unlock()
}

func (q *qSnapShot) String() string {
	q.qmu.Lock()
	defer q.qmu.Unlock()

	asStr, err := json.Marshal(q.queueSnapshot)
	if err != nil {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Error("Snapshot encoding failed: ", err)
	}

	return string(asStr)
}

func newQueueSnapShot() *qSnapShot {
	return &qSnapShot{
		queueSnapshot: make(map[string]ttlIndexElement),
	}
}

// StorageAPI exposes the raw store getters and setters and adds a TTL handler
type StorageAPI struct {
	store         Store
	ttlIndex      *fifo.Queue
	queueSnapshot *qSnapShot
	TTLChunkSize  int
}

func NewStorageAPI(store Store) *StorageAPI {
	thisSA := &StorageAPI{
		store:         store,
		ttlIndex:      fifo.NewQueue(),
		queueSnapshot: newQueueSnapShot(),
		TTLChunkSize:  100,
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.rafty.storage-api",
	}).Info("Starting TTL Processor")
	go thisSA.processTTLs()

	return thisSA
}

func (s *StorageAPI) GetKey(k string, evenIfExpired bool) (*KeyValueAPIObject, *ErrorResponse) {
	// Get the existing value
	v, errResp := s.getKeyFromStore(k)
	if errResp != nil {
		return nil, errResp
	}

	// Decode it
	returnValue, err := NewKeyValueAPIObjectFromMsgPack(v)
	if err != nil {
		return nil, NewErrorResponse("/"+k, "Key marshalling failed: "+err.Error())

	}

	if time.Now().After(returnValue.Node.Expiration) && returnValue.Node.TTL != 0 && evenIfExpired == false {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Debug("KEY EXISTS BUT HAS EXPIRED")
		return nil, NewErrorNotFound("/" + k)
	}

	return returnValue, nil
}

func (s *StorageAPI) SetKey(k string, value *rafty_objects.NodeValue, overwrite bool) (*rafty_objects.NodeValue, *ErrorResponse) {
	// Don't allow overwriting unless expired
	existingValue, errResp := s.getKeyFromStore(k)
	asNode, checkErr := NewKeyValueAPIObjectFromMsgPack(existingValue)

	var allowOverwrite = overwrite
	if checkErr == nil {
		if time.Now().After(asNode.Node.Expiration) && asNode.Node.TTL != 0 {
			allowOverwrite = true
		}
	}

	if errResp == nil && allowOverwrite == false {
		keyExistsErr := NewErrorResponse("/"+k, RAFTErrorKeyExists)
		return nil, keyExistsErr
	}

	value.Key = k

	// Set expiry value
	value.CalculateExpiry()
	if !overwrite {
		value.Created = time.Now().Unix()
	}
	value.LastUpdated = time.Now().Unix()
	toStore, encErr := value.EncodeForStorage()

	if encErr != nil {
		return nil, NewErrorResponse("/"+k, "Could not encode payload for store: "+encErr.Error())
	}

	// Write data to the store
	if err := s.store.Set(k, toStore); err != nil {
		return nil, NewErrorResponse("/"+k, "Could not write to store: "+err.Error())
	}

	// Track the TTL
	if value.TTL > 0 && value.Key != TTLSNAPSHOT_KEY {
		s.trackTTLForKey(value.Key, value.Expiration.Unix())
	}

	return value, nil
}

func (s *StorageAPI) AddToSet(k string, value []byte) ([]byte, *ErrorResponse) {
	// Write data to the store
	if err := s.store.AddToSet(k, value); err != nil {
		return nil, NewErrorResponse("/"+k, "Could not add to set: "+err.Error())
	}

	return value, nil
}

func (s *StorageAPI) GetSet(k string) (map[interface{}]interface{}, *ErrorResponse) {
	// Write data to the store
	var value map[interface{}]interface{}
	var err error

	if value, err = s.store.GetSet(k); err != nil {
		return nil, NewErrorResponse("/"+k, "Could not get set: "+err.Error())
	}

	return value, nil
}

func (s *StorageAPI) LPush(key string, values ...interface{}) *ErrorResponse {
	if err := s.store.LPush(key, values...); err != nil {
		return NewErrorResponse("/"+key, "Could not get set: "+err.Error())
	}

	return nil
}

func (s *StorageAPI) LLen(key string) (int64, *ErrorResponse) {
	var value int64
	var err error

	if value, err = s.store.LLen(key); err != nil {
		return value, NewErrorResponse("/"+key, "Could not get set: "+err.Error())
	}

	return value, nil
}

func (s *StorageAPI) LRem(key string, count int, value interface{}) *ErrorResponse {
	if err := s.store.LRem(key, count, value); err != nil {
		return NewErrorResponse("/"+key, "Could not get set: "+err.Error())
	}

	return nil
}

func (s *StorageAPI) LRange(key string, from, to int) ([]interface{}, *ErrorResponse) {
	var value []interface{}
	var err error

	if value, err = s.store.LRange(key, from, to); err != nil {
		return value, NewErrorResponse("/"+key, "Could not get set: "+err.Error())
	}

	return value, nil
}

func (s *StorageAPI) DeleteKey(k string) (*KeyValueAPIObject, *ErrorResponse) {
	if err := s.store.Delete(k); err != nil {
		return nil, NewErrorResponse("/"+k, "Delete failed: "+err.Error())
	}

	return nil, nil
}

func (s *StorageAPI) getKeyFromStore(k string) ([]byte, *ErrorResponse) {
	if k == "" {
		return nil, NewErrorResponse("/"+k, "Key cannot be empty")
	}

	v, err := s.store.Get(k)
	if err != nil {
		thisErr := NewErrorNotFound("/" + k)
		thisErr.MetaData = err.Error()
		return nil, thisErr
	}

	if v == nil {
		thisErr := NewErrorNotFound("/" + k)
		return nil, thisErr
	}

	return v, nil
}

type ttlIndexElement struct {
	TTL   int64
	Key   string
	Index int
}

func (s *StorageAPI) trackTTLForKey(key string, expires int64) {
	s.addTTL(ttlIndexElement{TTL: expires, Key: key})
}

func (s *StorageAPI) addTTL(elem ttlIndexElement) {
	if s.store.IsLeader() == false {
		return
	}

	s.ttlIndex.Add(elem)

	// Store the change in our snapshot
	elem.Index = s.ttlIndex.Len()

	log.Debug("Storing: ", elem, " in queue snapshot")
	s.queueSnapshot.SetElement(elem.Key, elem)

	log.Debug("This snaphot contains: ", s.queueSnapshot.queueSnapshot)

}

func (s *StorageAPI) processTTLElement() {
	if s.store.IsLeader() == false {
		return
	}

	max := s.TTLChunkSize
	if s.ttlIndex.Len() < s.TTLChunkSize {
		max = s.ttlIndex.Len()
	}
	applyDeletes := make([]ttlIndexElement, max)
	for i := 0; i < max; i++ {
		var skip bool
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Debug("Getting next element")
		thisElem := s.ttlIndex.Next()

		if thisElem == nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.rafty.storage-api",
			}).Debug("Element is empty - end of TTL queue")

			// End of list, break out, no need to continue
			break
		}

		existingKey, getErr := s.GetKey(thisElem.(ttlIndexElement).Key, true)
		if getErr != nil {
			// can't get the key, no need to delete it
			skip = true
		}

		if !skip {
			if existingKey.Node.Expiration.Unix() != thisElem.(ttlIndexElement).TTL {
				// Expiration has changed, so it must be in the queue again
				log.WithFields(logrus.Fields{
					"prefix": "tcf.rafty.storage-api",
				}).Debug("Skipping eviction for key, TTL has changed")
				skip = true
			}
		}

		if !skip {
			// Check expiry
			tn := time.Now().Unix()
			log.WithFields(logrus.Fields{
				"prefix": "tcf.rafty.storage-api",
			}).Debug("Exp is: ", thisElem.(ttlIndexElement).TTL)
			log.WithFields(logrus.Fields{
				"prefix": "tcf.rafty.storage-api",
			}).Debug("Now is: ", tn)

			if tn > thisElem.(ttlIndexElement).TTL {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.rafty.storage-api",
				}).Info("-> Removing key (", thisElem.(ttlIndexElement).Key, ") because expired")
				s.DeleteKey(thisElem.(ttlIndexElement).Key)

				// It's not in the queue, aso it shouldn't be in the snapshot
				applyDeletes[i] = thisElem.(ttlIndexElement)
			} else {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.rafty.storage-api",
				}).Debug("Expiry not reached yet, adding back to stack")
				s.addTTL(thisElem.(ttlIndexElement))
			}
		}

	}

	// Lets apply the delete operations to our queue
	for _, elem := range applyDeletes {
		s.queueSnapshot.qmu.Lock()
		delete(s.queueSnapshot.queueSnapshot, elem.Key)
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Debug("Updated queue snapshot in-mem, deleted: ", elem.Key)
		s.queueSnapshot.qmu.Unlock()
	}

	// Store the bulk snapshot change
	s.storeTTLSnapshot()
}

func (s *StorageAPI) rebuildFromSnapshot(intoFifoQ *fifo.Queue) SnapshotStatus {
	if s.store.IsLeader() == false {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Error("Can't rebuild: Not leader")
		return StatusSnapshotNotLeader
	}

	existingSnapShot, err := s.GetKey(TTLSNAPSHOT_KEY, false)
	if err != nil {
		return StatusSnapshotNotFound
	}

	if existingSnapShot == nil {
		return StatusSnapshotNotFound
	}

	var snapShotAsArray map[string]ttlIndexElement
	decErr := json.Unmarshal([]byte(existingSnapShot.Node.Value), &snapShotAsArray)
	if decErr != nil {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Error("Failed to decode snapshot backup")
		return StatusSnapshotNotFound
	}

	log.WithFields(logrus.Fields{
		"prefix": "tcf.rafty.storage-api",
	}).Info("Found: ", len(snapShotAsArray), " elements in snapshot.")

	for _, elem := range snapShotAsArray {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Debug("ADDING SNAPSHOT ELEMENT: ", elem.Key)
		intoFifoQ.Add(elem)
		s.queueSnapshot.SetElement(elem.Key, elem)
	}

	return StatusSnapshotFound
}

func (s *StorageAPI) storeTTLSnapshot() {
	if s.store.IsLeader() == false {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.storage-api",
		}).Error("Failed to store snapshot, not leader.")
		return
	}

	asStr := s.queueSnapshot.String()

	newSnapShotAPIObject := NewKeyValueAPIObject()
	newSnapShotAPIObject.Node.Key = TTLSNAPSHOT_KEY

	log.Debug("STORING TTL SNAPSHOT: ", asStr)
	newSnapShotAPIObject.Node.Value = asStr
	s.SetKey(TTLSNAPSHOT_KEY, newSnapShotAPIObject.Node, true)
}

func (s *StorageAPI) processTTLs() {
	for {
		if s.store.IsLeader() {
			// No queue? try to rebuild or reset it
			if s.queueSnapshot == nil {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.rafty.storage-api",
				}).Info("TTL Queue is empty, attempting to rebuild")
				s.queueSnapshot = newQueueSnapShot()
				status := s.rebuildFromSnapshot(s.ttlIndex)
				if status == StatusSnapshotNotFound {
					log.WithFields(logrus.Fields{
						"prefix": "tcf.rafty.storage-api",
					}).Info("No snapshot found, initialising a fresh queue")
					s.storeTTLSnapshot()
				}
			}

			// Process the next TTL item (but store snapshot in case we fail)
			log.WithFields(logrus.Fields{
				"prefix": "tcf.rafty.storage-api",
			}).Debug("Processing TTLs")
			s.processTTLElement()
		} else {
			// Not a leader, kill the queue we have
			s.queueSnapshot = nil
		}

		// Process every few seconds
		time.Sleep(5 * time.Second)
	}
}
