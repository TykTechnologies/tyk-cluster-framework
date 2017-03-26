package client

import "github.com/TykTechnologies/tyk-cluster-framework/payloads"

// PayloadHandler is a signature type for a function that handles a queue payload
type PayloadHandler func(payload payloads.Payload)
