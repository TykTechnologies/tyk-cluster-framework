package httpd

import (
	"net/http"
	"github.com/TykTechnologies/tyk-cluster-framework/rafty/store"
	"net/http/httputil"
	"net/url"
)

func (s *Service) forwardRequest(w http.ResponseWriter, r *http.Request) {

	trans := "http"
	if r.TLS != nil {
		trans = "https"
	}

	apiHost := store.GetHttpAPIFromRaftURL(s.store.Leader())
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

