package rafty

import (
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/http"
)

// Config defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

type Config struct {
	HttpServerAddr        string
	RaftServerAddress     string
	RaftBindToAddress     string

	JoinTimeout           int
	RaftDir               string
	TLSConfig             *httpd.TLSConfig
	RunInSingleServerMode bool
	ResetPeersOnLoad      bool
	AdvertiseInternal     bool
}

var tcfRaftyConfig Config = Config{
	HttpServerAddr:    DefaultHTTPAddr,
	RaftServerAddress: DefaultRaftAddr,
	JoinTimeout:       60,
	RaftDir:           "raft",
}

func init() {}
