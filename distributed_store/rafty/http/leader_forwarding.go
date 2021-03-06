package httpd

import (
	"github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/store"
	"net/http"
	"net/http/httputil"
	"net/url"
)

type CallType string

const (
	CallGetKey    CallType = "get"
	CallUpdateKey CallType = "update"
	CallDeleteKey CallType = "delete"
)

func (s *Service) forwardRequest(w http.ResponseWriter, r *http.Request) {

	trans := "http"
	if r.TLS != nil {
		trans = "https"
	}

	apiHost := store.GetHttpAPIFromRaftURL(s.store.Leader())
	if apiHost == "" {
		log.Error("Leader unknown, won't forward")
		return
	}

	targetAddr := trans + "://" + apiHost

	asURL, urlErr := url.Parse(targetAddr)
	if urlErr != nil {
		log.Error("Failed to generate leader HTTP address: ", urlErr, " was: ", targetAddr)
		s.writeToClient(w, r, NewErrorResponse(r.URL.Path, "Failed to forward to leader"), http.StatusInternalServerError)
		return
	}

	thisProxy := httputil.NewSingleHostReverseProxy(asURL)
	thisProxy.ServeHTTP(w, r)
}
