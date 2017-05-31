package store

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/spaolacci/murmur3"
	"github.com/wangjia184/sortedset"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
)

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := msgpack.Unmarshal(l.Data, &c); err != nil {
		log.Fatalf(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	case "addToSet":
		return f.applyAddToSet(c.Key, c.Value)
	case "lpush":
		return f.applyLPush(c.Key, c.Value)
	case "lrem":
		return f.applyLRem(c.Key, c.Count, c.Value)
	case "zadd":
		return f.applyZADD(c.Key, c.Score, c.Value)
	case "zremrangebyscore":
		return f.applyZREMRANGEBYSCORE(c.Key, c.Min, c.Max)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string][]byte)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string][]byte)
	if err := msgpack.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

func (f *fsm) applyAddToSet(key string, value []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, found := f.m[key]

	// Decode it from []byte
	var set map[interface{}]interface{}
	if !found {
		set = make(map[interface{}]interface{})
	} else {
		if err := msgpack.Unmarshal(v, &set); err != nil {
			return err
		}
	}

	// Set the key
	h := murmur3.New128()
	h.Write(value)
	hash := hex.EncodeToString(h.Sum(nil))
	set[hash] = value

	// Re-encode
	encoded, err := msgpack.Marshal(set)
	if err != nil {
		return err
	}

	f.m[key] = encoded
	return nil
}

func (f *fsm) insert(v []interface{}, x interface{}, i int) []interface{} {
	v = append(v, 0)
	copy(v[i+1:], v[i:])
	v[i] = x
	return v
}

func (f *fsm) applyLPush(key string, values []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, found := f.m[key]

	var l, vlist []interface{}
	if !found {
		l = make([]interface{}, 0)
	} else {
		if err := msgpack.Unmarshal(v, &l); err != nil {
			return err
		}
	}

	if err := msgpack.Unmarshal(values, &vlist); err != nil {
		return err
	}

	for _, value := range vlist {
		l = f.insert(l, value, 0)
	}

	// Re-encode
	encoded, err := msgpack.Marshal(l)
	if err != nil {
		return err
	}

	f.m[key] = encoded
	return nil
}

func (f *fsm) applyLRem(key string, count int, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, found := f.m[key]

	var l []interface{}
	if !found {
		return nil
	}

	if err := msgpack.Unmarshal(v, &l); err != nil {
		return err
	}

	var compValue interface{}
	if err := msgpack.Unmarshal(value, &compValue); err != nil {
		return err
	}

	var newL []interface{} = make([]interface{}, 0)

	var end, direction int
	end = len(l) - 1

	if count == 0 {
		direction = 1
	} else if count < 0 {
		direction = -1
	} else if count > 0 {
		direction = 1
	}

	c := 0

	processItem := func(i int) {
		if l[i] != compValue {
			x := l[i]
			if direction == 1 {
				newL = append(newL, x)
			} else {
				newL = append([]interface{}{x}, newL...)
			}
			return
		}
		c += 1
		if c == count && count != 0 {
			return
		}
	}

	if direction > 0 {
		for i := 0; i <= end; i++ {
			processItem(i)
		}
	} else {
		for i := end; i >= 0; i-- {
			processItem(i)
		}
	}

	// Re-encode
	encoded, err := msgpack.Marshal(newL)
	if err != nil {
		return err
	}

	f.m[key] = encoded
	return nil
}

type SortedSetBaseValue struct {
	ID    string
	Value interface{}
	Score sortedset.SCORE
}

func (f *fsm) applyZADD(key string, score int64, value []byte) error {
	// Get the encoded sorted set:
	f.mu.Lock()
	defer f.mu.Unlock()

	v, found := f.m[key]

	var l []SortedSetBaseValue
	if !found {
		l = make([]SortedSetBaseValue, 0)
	} else {
		if err := msgpack.Unmarshal(v, &l); err != nil {
			return err
		}
	}

	// Seed the set
	set := sortedset.New()
	for _, item := range l {
		set.AddOrUpdate(item.ID, item.Score, item.Value)
	}

	// USe an md5 to represent value, otherwise add or update wont update
	vid := fmt.Sprintf("%x", md5.Sum(value))
	set.AddOrUpdate(vid, sortedset.SCORE(score), value)

	// Dump the set
	allNodes := set.GetByRankRange(1, -1, false)
	count := set.GetCount()

	toStore := make([]SortedSetBaseValue, count)
	for i, node := range allNodes {
		toStore[i] = SortedSetBaseValue{
			ID:    node.Key(),
			Score: node.Score(),
			Value: node.Value,
		}
	}

	// Re-encode
	encoded, err := msgpack.Marshal(toStore)
	if err != nil {
		return err
	}

	f.m[key] = encoded
	return nil
}

func (f *fsm) applyZREMRANGEBYSCORE(key string, min int64, max int64) error {
	// Get the encoded sorted set:
	f.mu.Lock()
	defer f.mu.Unlock()

	v, found := f.m[key]

	var l []SortedSetBaseValue
	if !found {
		return nil
	}

	if err := msgpack.Unmarshal(v, &l); err != nil {
		return err
	}

	// Seed the set
	set := sortedset.New()
	for _, item := range l {
		set.AddOrUpdate(item.ID, item.Score, item.Value)
	}

	r := set.GetByScoreRange(sortedset.SCORE(min), sortedset.SCORE(max), nil)
	for _, i := range r {
		set.Remove(i.Key())
	}

	// Dump the set
	allNodes := set.GetByRankRange(1, -1, false)
	count := set.GetCount()

	toStore := make([]SortedSetBaseValue, count)
	for i, node := range allNodes {
		toStore[i] = SortedSetBaseValue{
			ID:    node.Key(),
			Score: node.Score(),
			Value: node.Value,
		}
	}

	// Re-encode
	encoded, err := msgpack.Marshal(toStore)
	if err != nil {
		return err
	}

	f.m[key] = encoded
	return nil
}
