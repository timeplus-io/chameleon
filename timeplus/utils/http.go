package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTPClientConfig struct {
	InsecureSkipVerify  bool
	MaxIdleConns        int
	MaxConnsPerHost     int
	MaxIdleConnsPerHost int
	Timeout             time.Duration
}

func NewDefaultHTTPClientConfig() *HTTPClientConfig {
	return &HTTPClientConfig{
		InsecureSkipVerify:  true,
		MaxIdleConns:        100,
		MaxConnsPerHost:     100,
		MaxIdleConnsPerHost: 100,
		Timeout:             10 * time.Second,
	}
}

func NewHttpClient(config HTTPClientConfig) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig.InsecureSkipVerify = config.InsecureSkipVerify
	t.MaxIdleConns = config.MaxIdleConns
	t.MaxConnsPerHost = config.MaxConnsPerHost
	t.MaxIdleConnsPerHost = config.MaxIdleConnsPerHost

	return &http.Client{
		Timeout:   config.Timeout,
		Transport: t,
	}
}

func NewDefaultHttpClient() *http.Client {
	config := NewDefaultHTTPClientConfig()
	return NewHttpClient(*config)
}

func HttpRequest(method string, url string, payload interface{}, client *http.Client) (int, []byte, error) {
	return HttpRequestWithHeader(method, url, payload, client, map[string]string{})
}

func HttpRequestWithToken(method string, url string, payload interface{}, client *http.Client, token string) (int, []byte, error) {
	headers := make(map[string]string)
	headers["Authorization"] = fmt.Sprintf("Bearer %s", token)

	return HttpRequestWithHeader(method, url, payload, client, headers)
}

func HttpRequestWithAPIKey(method string, url string, payload interface{}, client *http.Client, key string) (int, []byte, error) {
	headers := make(map[string]string)
	headers["X-Api-key"] = key

	return HttpRequestWithHeader(method, url, payload, client, headers)
}

// request will propragate error if the response code is not 2XX
func HttpRequestWithHeader(method string, url string, payload interface{}, client *http.Client, headers map[string]string) (int, []byte, error) {
	var body io.Reader
	if payload == nil {
		body = nil
	} else {
		jsonPostValue, _ := json.Marshal(payload)
		body = bytes.NewBuffer(jsonPostValue)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	for key := range headers {
		req.Header.Set(key, headers[key])
	}

	res, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}

	defer res.Body.Close()
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, nil, err
	}

	if res.StatusCode > 299 || res.StatusCode < 200 {
		return res.StatusCode, resBody, fmt.Errorf("request failed with status code %d, response body %s", res.StatusCode, resBody)
	}

	return res.StatusCode, resBody, nil
}
