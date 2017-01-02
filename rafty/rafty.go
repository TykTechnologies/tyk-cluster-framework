package rafty

// Config defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

type Config struct {
	HttpServerAddr string
	RaftServerAddress string
	JoinTimeout int
	RaftDir string
}

var tcfRaftyConfig Config = Config{
	HttpServerAddr: DefaultHTTPAddr,
	RaftServerAddress: DefaultRaftAddr,
	JoinTimeout: 60,
	RaftDir: "raft",
}

func init() {}