package raftyclient

import (
	"encoding/json"
	"github.com/TykTechnologies/tyk-cluster-framework/rafty/http"
	"io/ioutil"
	"net/http"
	"strings"

	"bytes"
	"errors"
	"fmt"
	"net/url"
	"time"
)

type APIClient struct {
	targetURL string
}

func init() {}

func NewRaftyClient(target string) *APIClient {
	target = strings.TrimSuffix(target, "/")
	target = target + "/key"
	return &APIClient{
		targetURL: target,
	}
}

func (c *APIClient) processErrorResponse(resp *http.Response) error {
	errBody, eBerr := ioutil.ReadAll(resp.Body)

	fmt.Println(string(errBody))

	if eBerr != nil {
		return eBerr
	}

	apiErrObject := &httpd.ErrorResponse{}
	nmErr := json.Unmarshal(errBody, apiErrObject)
	if nmErr != nil {
		return nmErr
	}

	return errors.New(apiErrObject.String())
}

func (c *APIClient) GetKey(key string) (*httpd.KeyValueAPIObject, error) {
	u := c.targetURL + "/" + key

	thisHttpRequest, rErr := http.NewRequest("GET", u, nil)
	if rErr != nil {
		return nil, rErr
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	resp, respErr := client.Do(thisHttpRequest)
	if respErr != nil {
		return nil, respErr
	}

	if resp.StatusCode != 200 {
		return nil, c.processErrorResponse(resp)

	}

	body, bErr := ioutil.ReadAll(resp.Body)
	if bErr != nil {
		return nil, bErr
	}

	newAPIReturnObject := httpd.NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) DeleteKey(key string) (*httpd.KeyValueAPIObject, error) {
	u := c.targetURL + "/" + key
	thisHttpRequest, rErr := http.NewRequest("DELETE", u, nil)
	if rErr != nil {
		return nil, rErr
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	resp, respErr := client.Do(thisHttpRequest)
	if respErr != nil {
		return nil, respErr
	}

	if resp.StatusCode != 200 {
		return nil, c.processErrorResponse(resp)
	}

	body, bErr := ioutil.ReadAll(resp.Body)
	if bErr != nil {
		return nil, bErr
	}

	newAPIReturnObject := httpd.NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) CreateKey(key string, value interface{}, ttl string) (*httpd.KeyValueAPIObject, error) {

	valueAsJSON, encErr := json.Marshal(value)
	if encErr != nil {
		return nil, encErr
	}

	valueToSend := string(valueAsJSON)

	vals := url.Values{}
	if ttl != "" {
		vals.Add("ttl", ttl)
	}

	vals.Add("value", valueToSend)

	u := c.targetURL + "/" + key
	thisHttpRequest, rErr := http.NewRequest("POST", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	if rErr != nil {
		return nil, rErr
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	resp, respErr := client.Do(thisHttpRequest)
	if respErr != nil {
		return nil, respErr
	}

	if resp.StatusCode != 201 {
		return nil, c.processErrorResponse(resp)
	}

	body, bErr := ioutil.ReadAll(resp.Body)
	if bErr != nil {
		return nil, bErr
	}

	newAPIReturnObject := httpd.NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) UpdateKey(key string, value interface{}, ttl string) (*httpd.KeyValueAPIObject, error) {
	valueAsJSON, encErr := json.Marshal(value)
	if encErr != nil {
		return nil, encErr
	}

	valueToSend := string(valueAsJSON)

	vals := url.Values{}
	if ttl != "" {
		vals.Add("ttl", ttl)
	}

	vals.Add("value", valueToSend)

	u := c.targetURL + "/" + key
	thisHttpRequest, rErr := http.NewRequest("PUT", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	if rErr != nil {
		return nil, rErr
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	resp, respErr := client.Do(thisHttpRequest)
	if respErr != nil {
		return nil, respErr
	}

	if resp.StatusCode != 200 {
		return nil, c.processErrorResponse(resp)
	}

	body, bErr := ioutil.ReadAll(resp.Body)
	if bErr != nil {
		return nil, bErr
	}

	newAPIReturnObject := httpd.NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}
