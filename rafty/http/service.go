// Package httpd provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package httpd

import (
	"encoding/json"
	"net/http"
	logger "github.com/TykTechnologies/tykcommon-logger"
	rafty_objects "github.com/TykTechnologies/tyk-cluster-framework/rafty/objects"
	"github.com/TykTechnologies/logrus"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"gopkg.in/vmihailenco/msgpack.v2"
	"fmt"
	"time"
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
}

type TLSConfig struct {
	KeyFile string
	CertFile string
}

// Service provides HTTP service.
type Service struct {
	addr string
	tlsConfig *TLSConfig

	store Store
}

// New returns an uninitialized HTTP service.
func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

// Start starts the service.
func (s *Service) Start() error {

	r := mux.NewRouter()
	r.HandleFunc("/join", s.handleJoin).Methods("POST")
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


func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
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
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) getKeyFromStore(k string) ([]byte, *ErrorResponse) {
	if k == "" {
		return nil, NewErrorResponse("/"+k, "Key cannot be empty")
	}

	v, err := s.store.Get(k)
	if err != nil {
		thisErr := NewErrorNotFound("/"+k)
		thisErr.MetaData = err.Error()
		return nil, thisErr
	}

	if v == nil {
		thisErr := NewErrorNotFound("/"+k)
		return nil, thisErr
	}

	return v, nil
}

func (s *Service) handleGetKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for GET"), http.StatusBadRequest)
		return
	}

	// Get the existing value
	v, errResp := s.getKeyFromStore(k)
	if errResp != nil {
		s.writeToClient(w, r, errResp, http.StatusNotFound)
		return
	}

	// Decode it
	returnValue, err := NewKeyValueAPIObjectFromMsgPack(v)
	if err != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Key marshalling failed: " + err.Error()), http.StatusInternalServerError)
		return
	}

	if time.Now().After(returnValue.Node.Expiration) {
		s.writeToClient(w, r, NewErrorNotFound("/"+k), http.StatusNotFound)
		return
	}

	s.writeToClient(w, r, returnValue, 200)
}

func (s *Service) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for POST"), http.StatusBadRequest)
		return
	}

	// Don't allow overwriting unless expired
	existingValue, errResp := s.getKeyFromStore(k)
	asNode, checkErr := NewKeyValueAPIObjectFromMsgPack(existingValue)

	var allowOverwrite bool
	if checkErr == nil {
		if time.Now().After(asNode.Node.Expiration) {
			allowOverwrite = true
		}
	}

	if errResp == nil && allowOverwrite == false {
		s.writeToClient(w, r, NewErrorResponse("Key already exists", ""), http.StatusNotFound)
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

	// Set expiry value
	nodeData.CalculateExpiry()
	nodeData.Created = time.Now().Unix()
	toStore, encErr := nodeData.EncodeForStorage()

	if encErr != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not encode payload for store: "+encErr.Error()), http.StatusBadRequest)
		return
	}

	// Write data to the store
	if err := s.store.Set(k, toStore); err != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not write to store: "+err.Error()), http.StatusInternalServerError)
		return
	}

	// Return a successful create
	returnData := NewKeyValueAPIObjectWithAction(ActionKeyCreated)
	returnData.Node = &nodeData

	s.writeToClient(w, r, returnData, 201)
}

func (s *Service) handleUpdateKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for POST"), http.StatusBadRequest)
		return
	}

	// Get the existing value
	v, errResp := s.getKeyFromStore(k)
	if errResp != nil {
		s.writeToClient(w, r, errResp, http.StatusNotFound)
		return
	}

	// Decode it
	var nodeValue rafty_objects.NodeValue
	mDecErr := msgpack.Unmarshal(v, &nodeValue)
	if mDecErr != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Key marshalling failed: " + mDecErr.Error()), http.StatusInternalServerError)
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

	toStore, encErr := nodeValue.EncodeForStorage()
	if encErr != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not encode payload for store: "+encErr.Error()), http.StatusBadRequest)
		return
	}

	// Write data to the store
	if err := s.store.Set(k, toStore); err != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Could not write to store: "+err.Error()), http.StatusInternalServerError)
		return
	}

	// Return ok
	returnData := NewKeyValueAPIObjectWithAction(ActionKeyModified)
	returnData.Node = &nodeValue

	s.writeToClient(w, r, returnData, 201)
}

func (s *Service) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k := vars["name"]
	if k == "" {
		s.writeToClient(w, r, NewErrorResponse("/", "key cannot be empty for POST"), http.StatusBadRequest)
		return
	}

	if err := s.store.Delete(k); err != nil {
		s.writeToClient(w, r, NewErrorResponse("/"+k, "Delete failed: " + err.Error()), http.StatusInternalServerError)
		return
	}
	s.store.Delete(k)

	delResp := NewKeyValueAPIObjectWithAction(ActionKeyDeleted)
	delResp.Node.Key = "/"+k

	s.writeToClient(w, r, delResp, http.StatusInternalServerError)
}

//// Addr returns the address on which the Service is listening
//func (s *Service) Addr() net.Addr {
//	return s.ln.Addr()
//}