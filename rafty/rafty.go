package rafty

import (
	"github.com/TykTechnologies/tyk-cluster-framework/rafty/http"
)

// Config defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

type Config struct {
	HttpServerAddr        string
	RaftServerAddress     string
	JoinTimeout           int
	RaftDir               string
	TLSConfig             *httpd.TLSConfig
	RunInSingleServerMode bool
	ResetPeersOnLoad bool
}

var tcfRaftyConfig Config = Config{
	HttpServerAddr:    DefaultHTTPAddr,
	RaftServerAddress: DefaultRaftAddr,
	JoinTimeout:       60,
	RaftDir:           "raft",
}

func init() {}
