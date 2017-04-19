package helpers

import (
	"net/url"
	"strings"
)

// This is for go 1.7 compatibility only
type ExtendedURL struct {
	URL *url.URL
}

func stripPort(hostport string) string {
	colon := strings.IndexByte(hostport, ':')
	if colon == -1 {
		return hostport
	}
	if i := strings.IndexByte(hostport, ']'); i != -1 {
		return strings.TrimPrefix(hostport[:i], "[")
	}
	return hostport[:colon]
}

func portOnly(hostport string) string {
	colon := strings.IndexByte(hostport, ':')
	if colon == -1 {
		return ""
	}
	if i := strings.Index(hostport, "]:"); i != -1 {
		return hostport[i+len("]:"):]
	}
	if strings.Contains(hostport, "]") {
		return ""
	}
	return hostport[colon+len(":"):]
}


func (e *ExtendedURL) Port() string {
	return portOnly(e.URL.Host)
}

func (e *ExtendedURL) Hostname() string {
	return stripPort(e.URL.Host)
}
