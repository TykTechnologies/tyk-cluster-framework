package encoding

// We can probably get rid of this, it isn't very ueful

type Encoding string

const (
	JSON Encoding = "application/json"
	MPK  Encoding = "application/msgpack"
	NONE Encoding = "byte"
)
