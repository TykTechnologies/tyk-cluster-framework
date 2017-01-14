package objects

import (
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

type NodeValue struct {
	Expiration  time.Time `json:"expiration,omitempty"`
	TTL         int       `json:"ttl,omitempty"`
	Key         string    `json:"key"`
	Value       string    `json:"value"`
	Created     int64     `json:"created"`
	LastUpdated int64     `json:"lastUpdated"`
}

func (n *NodeValue) CalculateExpiry() {
	n.Expiration = time.Now().Add(time.Duration(n.TTL) * time.Second)

}

func (n *NodeValue) EncodeForStorage() ([]byte, error) {
	n.LastUpdated = time.Now().Unix()
	b, err := msgpack.Marshal(n)
	if err != nil {
		return nil, err
	}
	return b, nil
}
