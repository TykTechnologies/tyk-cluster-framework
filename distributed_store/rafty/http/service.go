// Package httpd provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package httpd

import (
	"encoding/json"
	"fmt"
	"github.com/TykTechnologies/logrus"
	rafty_objects "github.com/TykTechnologies/tyk-cluster-framework/distributed_store/rafty/objects"
	logger "github.com/TykTechnologies/tykcommon-logger"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"gopkg.in/vmihailenco/msgpack.v2"
	"net/http"
)

var log = logger.GetLogger()

// Store is the interface Raft-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(key string) ([]byte, error)

	// Set sets the value for the given key, via distributed consensus.
	Set(key string, value []byte) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error

	// Returns whether the store is leader or not
	IsLeader() bool

	// RemovePeer removes a peer
	RemovePeer(string) error

	// Leader returns th leader addr
	Leader() string

	SetPeers([]string) error

	AddToSet(key string, value []byte) error

	// Set and list operations
	GetSet(string) (map[interface{}]interface{}, error)
	LPush(string, ...interface{}) error
	LLen(string) (int64, error)
	LRem(string, int, interface{}) error
	LRange(key string, from, to int) ([]interface{}, error)
}

type TLSConfig struct {
	KeyFile  string
	CertFile string
}

// Service provides HTTP service.
type Service struct {
	addr      string
	tlsConfig *TLSConfig

	store       Store
	StorageAPI  *StorageAPI
	EmbeddedAPI *EmbeddedService
}

// New returns an uninitialized HTTP service.
func New(addr string, store Store, tlsConfig *TLSConfig) *Service {
	var tls bool
	if tlsConfig != nil {
		tls = true
	}

	sAPI := NewStorageAPI(store)
	eAPI := NewEmbeddedService(tls, sAPI)

	return &Service{
		addr:        addr,
		store:       store,
		StorageAPI:  sAPI,
		EmbeddedAPI: eAPI,
		tlsConfig:   tlsConfig,
	}
}

// Start starts the service.
func (s *Service) Start() error {

	r := mux.NewRouter()
	r.HandleFunc("/join", s.handleJoin).Methods("POST")
	r.HandleFunc("/leader", s.handleIsLeader).Methods("GET")
	r.HandleFunc("/setpeers", s.setPeers).Methods("POST")
	r.HandleFunc("/remove", s.handleRemove).Methods("POST")
	r.HandleFunc("/key/{name}", s.handleGetKey).Methods("GET")
	r.HandleFunc("/key/{name}", s.handleUpdateKey).Methods("PUT")
	r.HandleFunc("/key/{name}", s.handleCreateKey).Methods("POST")
	r.HandleFunc("/key/{name}", s.handleDeleteKey).Methods("DELETE")

	go func() {
		// Check TLS
		if s.tlsConfig != nil {
			//  Start HTTPS
			err_https := http.ListenAndServeTLS(fmt.Sprintf("%s", s.addr), s.tlsConfig.CertFile, s.tlsConfig.KeyFile, r)
			if err_https != nil {
				log.WithFields(logrus.Fields{
					"prefix": "tcf.rafty.http",
				}).Fatal("Web server (HTTPS): ", err_https)
			}
			// Exit out of server loop on fail
			return
		}

		// By default, run HTTP
		err_http := http.ListenAndServe(fmt.Sprintf("%s", s.addr), r)
		if err_http != nil {
			log.WithFields(logrus.Fields{
				"prefix": "tcf.rafty.http",
			}).Fatal("Web server (HTTP): ", err_http)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	return
}

func (s *Service) writeToClient(w http.ResponseWriter, r *http.Request, responseObject interface{}, code int) {
	thisResponse, err := json.Marshal(responseObject)
	if err != nil {
		log.WithFields(logrus.Fields{
			"prefix": "tcf.rafty.http",
		}).Error("Response marshal error: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Generator", "tcf.rafty")
	w.WriteHeader(code)
	w.Write(thisResponse)
}

func (s *Service) setPeers(w http.ResponseWriter, r *http.Request) {
	var m []string
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.SetPeers(m); err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleIsLeader(w http.ResponseWriter, r *http.Request) {
	v := LeaderResponse{
		IsLeader: s.store.IsLeader(),
		LeaderIs: s.store.Leader(),
	}

	s.store.IsLeader()
	s.writeToClient(w, r, v, 200)
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	if !s.store.IsLeader() {
		s.forwardRequest(w, r)
		return
	}

	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(remoteAddr); err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleRemove(w http.ResponseWriter, r *http.Request) {
	if !s.store.IsLeader() {
		s.forwardRequest(w, r)
		return
	}

	log.Info("REMOVING PEER")
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.RemovePeer(remoteAddr); err != nil {
		log.Error("FAILED TO REMOVE PEER: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Info("PEER REMOVED")
}
func (s *Service) handleGetKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for GET"), http.StatusBadRequest)
		return
	}

	// Get the existing value
	returnValue, errResp := s.StorageAPI.GetKey(k, false)
	if errResp != nil {
		if errResp.ErrorCode == RAFTErrorNotFound {
			s.writeToClient(w, r, errResp, http.StatusNotFound)
			return
		}

		s.writeToClient(w, r, errResp, http.StatusBadRequest)
	}

	s.writeToClient(w, r, returnValue, 200)
}

func (s *Service) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	if !s.store.IsLeader() {
		s.forwardRequest(w, r)
		return
	}

	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for POST"), http.StatusBadRequest)
		return
	}

	// Read the parameters from the POST body as form params
	err := r.ParseForm()
	if err != nil {
		// Handle error
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not parse form data: "+err.Error()), http.StatusBadRequest)
		return
	}

	decoder := schema.NewDecoder()
	var nodeData rafty_objects.NodeValue
	decErr := decoder.Decode(&nodeData, r.PostForm)
	nodeData.Key = k

	if decErr != nil {
		// Handle error
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not decode form data: "+decErr.Error()), http.StatusBadRequest)
		return
	}

	// Write data to the store
	toReturn, errResp := s.StorageAPI.SetKey(k, &nodeData, false)
	if errResp != nil {
		if errResp.ErrorCode == RAFTErrorKeyExists {
			s.writeToClient(w, r, errResp, http.StatusBadRequest)
			return
		}

		s.writeToClient(w, r, errResp, http.StatusInternalServerError)
		return
	}

	// Return a successful create
	returnData := NewKeyValueAPIObjectWithAction(ActionKeyCreated)
	returnData.Node = toReturn
	s.writeToClient(w, r, returnData, 201)
}

func (s *Service) handleUpdateKey(w http.ResponseWriter, r *http.Request) {
	if !s.store.IsLeader() {
		s.forwardRequest(w, r)
		return
	}

	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for POST"), http.StatusBadRequest)
		return
	}

	// Get the existing value
	v, errResp := s.StorageAPI.getKeyFromStore(k)
	if errResp != nil {
		s.writeToClient(w, r, errResp, http.StatusNotFound)
		return
	}

	// Decode it
	var nodeValue rafty_objects.NodeValue
	mDecErr := msgpack.Unmarshal(v, &nodeValue)
	if mDecErr != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Key marshalling failed: "+mDecErr.Error()), http.StatusInternalServerError)
		return
	}

	// Read the parameters from the POST body as form params
	err := r.ParseForm()
	if err != nil {
		// Handle error
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not parse form data: "+err.Error()), http.StatusBadRequest)
		return
	}

	oldTTLVal := nodeValue.TTL
	decoder := schema.NewDecoder()
	// Read the form data into the existing value
	decErr := decoder.Decode(&nodeValue, r.PostForm)

	if decErr != nil {
		// Handle error
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not decode form data: "+decErr.Error()), http.StatusBadRequest)
		return
	}

	// Set expiry value if it has changed
	if oldTTLVal != nodeValue.TTL {
		nodeValue.CalculateExpiry()
	}

	// Write data to the store
	if _, err := s.StorageAPI.SetKey(k, &nodeValue, true); err != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not write to store: "+err.ErrorCode.Reason), http.StatusInternalServerError)
		return
	}

	// Return ok
	returnData := NewKeyValueAPIObjectWithAction(ActionKeyModified)
	returnData.Node = &nodeValue

	s.writeToClient(w, r, returnData, http.StatusOK)
}

func (s *Service) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	if !s.store.IsLeader() {
		s.forwardRequest(w, r)
		return
	}

	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for POST"), http.StatusBadRequest)
		return
	}

	if _, err := s.StorageAPI.DeleteKey(k); err != nil {
		s.writeToClient(w, r, err, http.StatusInternalServerError)
		return
	}

	delResp := NewKeyValueAPIObjectWithAction(ActionKeyDeleted)
	delResp.Node.Key = "/" + k

	s.writeToClient(w, r, delResp, http.StatusOK)
}
