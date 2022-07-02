package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/viper"
	"github.com/timeplus-io/chameleon/tsbs/log"
)

func NewDefaultHttpClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig.InsecureSkipVerify = true
	t.MaxIdleConns = viper.GetInt("http-max-idle-connection")
	t.MaxConnsPerHost = viper.GetInt("http-max-connection-per-host")
	t.MaxIdleConnsPerHost = viper.GetInt("http-max-idle-connection-per-host")

	return &http.Client{
		Timeout:   viper.GetDuration("http-timeout") * time.Second,
		Transport: t,
	}
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
		log.Logger().Debugf("send empty request to url %s", url)
	} else {
		jsonPostValue, _ := json.Marshal(payload)
		body = bytes.NewBuffer(jsonPostValue)
		log.Logger().Debugf("send request %s to url %s", string(jsonPostValue), url)
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
