package httpd

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"bytes"
	"errors"
	"fmt"
	"net/url"
	"strconv"
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

	apiErrObject := &ErrorResponse{}
	nmErr := json.Unmarshal(errBody, apiErrObject)
	if nmErr != nil {
		return nmErr
	}

	return errors.New(apiErrObject.String())
}

func (c *APIClient) GetKey(key string) (*KeyValueAPIObject, error) {
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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) DeleteKey(key string) (*KeyValueAPIObject, error) {
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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) CreateKey(key string, value string, ttl string) (*KeyValueAPIObject, error) {
	valueToSend := value

	vals := url.Values{}
	if ttl != "" {
		vals.Add("ttl", ttl)
	}

	vals.Add("value", valueToSend)

	u := c.targetURL + "/" + key
	thisHttpRequest, rErr := http.NewRequest("POST", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) UpdateKey(key string, value string, ttl string) (*KeyValueAPIObject, error) {
	valueToSend := value

	vals := url.Values{}
	if ttl != "" {
		vals.Add("ttl", ttl)
	}

	vals.Add("value", valueToSend)

	u := c.targetURL + "/" + key
	thisHttpRequest, rErr := http.NewRequest("PUT", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) AddToSet(key string, value []byte) (*KeyValueAPIObject, error) {
	// Value already encoded
	valueToSend := string(value)

	vals := url.Values{}
	vals.Add("value", valueToSend)

	u := c.targetURL + "/sadd/" + key
	thisHttpRequest, rErr := http.NewRequest("PUT", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) LPush(key string, values ...interface{}) (*KeyValueAPIObject, error) {
	vals := url.Values{}
	valueAsJSON, encErr := json.Marshal(values)
	if encErr != nil {
		return nil, encErr
	}
	vals.Add("value", string(valueAsJSON))

	u := c.targetURL + "/lpush/" + key
	thisHttpRequest, rErr := http.NewRequest("PUT", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) LRem(key string, count int, value interface{}) (*KeyValueAPIObject, error) {
	vals := url.Values{}
	valueAsJSON, encErr := json.Marshal(value)
	if encErr != nil {
		return nil, encErr
	}

	vals.Add("value", string(valueAsJSON))
	vals.Add("count", strconv.Itoa(count))

	u := c.targetURL + "/lrem/" + key
	thisHttpRequest, rErr := http.NewRequest("DELETE", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) ZAdd(key string, score int64, value interface{}) (*KeyValueAPIObject, error) {
	vals := url.Values{}
	valueAsJSON, encErr := json.Marshal(value)
	if encErr != nil {
		return nil, encErr
	}

	vals.Add("value", string(valueAsJSON))
	vals.Add("score", strconv.Itoa(int(score)))

	u := c.targetURL + "/zadd/" + key
	thisHttpRequest, rErr := http.NewRequest("PUT", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

func (c *APIClient) ZRemRangeByScore(key string, min int64, max int64) (*KeyValueAPIObject, error) {
	vals := url.Values{}
	vals.Add("min", strconv.Itoa(int(min)))
	vals.Add("max", strconv.Itoa(int(max)))

	u := c.targetURL + "/zremrangebyscore/" + key
	thisHttpRequest, rErr := http.NewRequest("PUT", u, bytes.NewBufferString(vals.Encode()))
	thisHttpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

	newAPIReturnObject := NewKeyValueAPIObject()

	mErr := json.Unmarshal(body, newAPIReturnObject)
	if mErr != nil {
		return nil, mErr
	}

	return newAPIReturnObject, nil
}

// TODO: Read commands for advanced objects
