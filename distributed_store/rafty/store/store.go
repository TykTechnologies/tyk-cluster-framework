// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"errors"
	"github.com/TykTechnologies/logrus"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/wangjia184/sortedset"
	"gopkg.in/vmihailenco/msgpack.v2"
	"strconv"
	"strings"
)

var log = logger.GetLogger()

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Count int    `json:"count,omitempty"`
	Score int64  `json:"score,omitempty"`
	Min   int64  `json:"min,omitempty"`
	Max   int64  `json:"max,omitempty"`
	Value []byte `json:"value,omitempty"`
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string

	mu sync.Mutex
	m  map[string][]byte // The key-value store for the system.

	raft *raft.Raft // The consensus mechanism

	logger *logrus.Logger
}

// New returns a new Store.
func New() *Store {
	return &Store{
		m:      make(map[string][]byte),
		logger: log,
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
	// Setup Raft configuration with our custom writer to convert to logrus
	config := raft.DefaultConfig()
	convertedLogger := &ConvertedLogrusLogger{Prefix: "tcf.rafty.raft", LogInstance: log}
	config.LogOutput = convertedLogger
	config.ShutdownOnRemove = false

	// Check for any existing peers.
	peers, err := ReadPeersJSON(filepath.Join(s.RaftDir, "peers.json"))
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	//if enableSingle && len(peers) <= 1 {
	if enableSingle && len(peers) <= 1 {
		s.logger.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.store",
		}).Info("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create peer storage.
	peerStore := raft.NewJSONPeers(s.RaftDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra
	return nil
}

func (s *Store) Leader() string {
	return s.raft.Leader()
}

// Get returns the value for the given key.
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set sets the value for the given key.
func (s *Store) Set(key string, value []byte) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := msgpack.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) AddToSet(key string, value []byte) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	// Wrap and apply
	c := &command{
		Op:    "addToSet",
		Key:   key,
		Value: value,
	}

	b, err := msgpack.Marshal(c)
	if err != nil {
		return err
	}

	m := s.raft.Apply(b, raftTimeout)
	return m.Error()
}

func (s *Store) GetSet(key string) (map[interface{}]interface{}, error) {
	// Get the set
	s.mu.Lock()
	v, f := s.m[key]
	s.mu.Unlock()

	// Decode it from []byte
	var set map[interface{}]interface{}
	if !f {
		return nil, nil
	}

	if err := msgpack.Unmarshal(v, &set); err != nil {
		return nil, err
	}

	return set, nil
}

func (s *Store) LPush(key string, values ...interface{}) error {
	// Encode
	encoded, err := msgpack.Marshal(values)
	if err != nil {
		return err
	}

	// Wrap and apply
	c := &command{
		Op:    "lpush",
		Key:   key,
		Value: encoded,
	}

	b, err := msgpack.Marshal(c)
	if err != nil {
		return err
	}

	m := s.raft.Apply(b, raftTimeout)
	return m.Error()
}

func (s *Store) LLen(key string) (int64, error) {
	// Get the list
	s.mu.Lock()
	v, f := s.m[key]
	s.mu.Unlock()

	var l []interface{}
	if !f {
		return 0, nil
	} else {
		if err := msgpack.Unmarshal(v, &l); err != nil {
			return 0, err
		}
	}

	return int64(len(l)), nil
}

func (s *Store) LRem(key string, count int, value interface{}) error {
	// Encode
	encoded, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	// Wrap and apply
	com := &command{
		Op:    "lrem",
		Count: count,
		Key:   key,
		Value: encoded,
	}

	b, err := msgpack.Marshal(com)
	if err != nil {
		return err
	}

	m := s.raft.Apply(b, raftTimeout)
	return m.Error()
}

func (s *Store) LRange(key string, from, to int) ([]interface{}, error) {

	// Get the list
	s.mu.Lock()
	v, f := s.m[key]
	s.mu.Unlock()

	var list []interface{}
	if !f {
		return []interface{}{}, nil
	} else {
		if err := msgpack.Unmarshal(v, &list); err != nil {
			return []interface{}{}, err
		}
	}

	if from < 0 {
		from = len(list) + from

		// Can't be smaller than 0!
		if from < 0 {
			from = 0
		}
	}

	// Can't be bigger than the list, return 0
	if from > len(list) {
		return []interface{}{}, errors.New("Start index larger than length of list")
	}

	if to < 0 {
		to = len(list) + to
	}

	// Rightmost index
	if to >= 0 {
		to += 1
	}

	if to > len(list) {
		to = len(list)
	}

	if from > to {
		return []interface{}{}, errors.New("Start index is after end")
	}

	return list[from:to], nil
}

func (s *Store) ZRangeByScore(key string, min, max int64) ([]interface{}, error) {
	// Get the list
	s.mu.Lock()
	v, f := s.m[key]
	s.mu.Unlock()

	var sSetSeed []SortedSetBaseValue
	if !f {
		return []interface{}{}, nil
	} else {
		if err := msgpack.Unmarshal(v, &sSetSeed); err != nil {
			return []interface{}{}, err
		}
	}

	// Seed the set
	set := sortedset.New()
	for _, item := range sSetSeed {
		set.AddOrUpdate(item.ID, item.Score, item.Value)
	}

	r := set.GetByScoreRange(sortedset.SCORE(min), sortedset.SCORE(max), nil)

	vals := make([]interface{}, len(r))
	for i, v := range r {
		var decoded interface{}
		if err := msgpack.Unmarshal(v.Value.([]byte), &decoded); err != nil {
			return []interface{}{}, err
		}
		vals[i] = decoded
	}

	return vals, nil
}

func (s *Store) ZAdd(key string, score int64, value interface{}) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	// Encode
	encoded, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	c := &command{
		Op:    "zadd",
		Key:   key,
		Score: score,
		Value: encoded,
	}
	b, err := msgpack.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) ZRemRangeByScore(key string, min int64, max int64) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "zremrangebyscore",
		Key: key,
		Min: min,
		Max: max,
	}
	b, err := msgpack.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := msgpack.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) error {
	s.logger.WithFields(logrus.Fields{
		"prefix": "tcf.rafty.store",
	}).Info("received join request for remote node as: ", addr)

	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.WithFields(logrus.Fields{
		"prefix": "tcf.rafty.store",
	}).Infof("node at %s joined successfully", addr)
	return nil
}

func (s *Store) SetPeers(addr []string) error {
	s.logger.WithFields(logrus.Fields{
		"prefix": "tcf.rafty.store",
	}).Info("received set peer request", addr)

	f := s.raft.SetPeers(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.WithFields(logrus.Fields{
		"prefix": "tcf.rafty.store",
	}).Infof("nodes set succesfully")
	return nil
}

func (s *Store) RemovePeer(addr string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	s.raft.RemovePeer(addr)
	return nil
}

func (s *Store) IsLeader() bool {
	if s.raft == nil {
		return false
	}
	if s.raft.State() == raft.Leader {
		return true
	}
	return false
}

type fsmSnapshot struct {
	store map[string][]byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := msgpack.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}

func ReadPeersJSON(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var peers []string
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}

func ResetPeersJSON(path string, hostname string) {
	peers, err := ReadPeersJSON(path)
	if err != nil {
		log.Fatal("Could not reset peers: ", err)
	}
	newPeers := []string{hostname}
	if len(peers) > 1 {
		b, encErr := json.Marshal(newPeers)
		if encErr != nil {
			log.Fatal("Could not marshal peers: ", encErr)
		}
		wErr := ioutil.WriteFile(path, b, 0644)
		if wErr != nil {
			log.Fatal("Could not write peers: ", wErr)
		}
	}
}

func GetHttpAPIFromRaftURL(leaderAddr string) string {
	urlParts := strings.Split(leaderAddr, ":")
	var host, portStr string
	if len(urlParts) > 1 {
		host = urlParts[0]
		portStr = urlParts[1]
	} else {
		host = leaderAddr
	}

	asInt, intErr := strconv.Atoi(portStr)
	if intErr != nil {
		log.Fatal(intErr, "was: ", portStr)
	}
	apiAddr := host + ":" + strconv.Itoa(asInt-100)

	return apiAddr
}
