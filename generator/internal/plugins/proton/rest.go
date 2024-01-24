package proton

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/utils"
)

const retries = 2

var ErrProtonDown = errors.New("failed to connect to Proton")

const (
	// versioned endpoints
	// example: http://host:port/proton/v1/ddl/streams
	StreamsPath = "ddl/streams"
	IngestPath  = "ingest/streams"
	SearchPath  = "search"

	// unversioned endpoints
	// example: http://host:port/proton/ping
	PingPath = "ping"
	InfoPath = "info"
)

type Client struct {
	host         string
	restUser     string
	restPassword string
	baseURL      string
	client       *http.Client
}

type ColumnDef struct {
	Name                    string `json:"name" binding:"required" example:"name"`
	Type                    string `json:"type" binding:"required" example:"string"`
	Default                 string `json:"default,omitempty"`
	Codec                   string `json:"codec,omitempty"`
	TTLExpression           string `json:"ttl_expression,omitempty"`
	SkippingIndexExpression string `json:"skipping_index_expression,omitempty"`

	// This is used by proton only.
	CompressionCodec string `json:"compression_codec,omitempty" swaggerignore:"true"`
} // @name ColumnDef

type StreamDef struct {

	// Stream name should only contain a maximum of 64 letters, numbers, or _, and start with a letter
	Name                   string      `json:"name" binding:"required" example:"test_stream"`
	Columns                []ColumnDef `json:"columns"`
	EventTimeColumn        string      `json:"event_time_column,omitempty"`
	Shards                 int         `json:"shards,omitempty"`
	ReplicationFactor      int         `json:"replication_factor,omitempty"`
	OrderByExpression      string      `json:"order_by_expression,omitempty"`
	OrderByGranularity     string      `json:"order_by_granularity,omitempty"`
	PartitionByGranularity string      `json:"partition_by_granularity,omitempty"`
	TTLExpression          string      `json:"ttl_expression,omitempty" example:"to_datetime(_tp_time) + INTERVAL 7 DAY"`

	// Storage mode of stream. Defaulted to `append`.
	Mode string `json:"mode,omitempty" example:"append" enums:"append,changelog,changelog_kv,versioned_kv"`

	// Expression of primary key, required in `changelog_kv` and `versioned_kv` mode
	PrimaryKey string `json:"primary_key,omitempty"`
} // @name StreamDef

type StreamStorageConfig struct {
	// The max size a stream can grow. Any non-positive value means unlimited size. Defaulted to 10 GiB.
	RetentionBytes int `json:"logstore_retention_bytes,omitempty" example:"10737418240"`

	// The max time the data can be retained in the stream. Any non-positive value means unlimited time. Defaulted to 7 days.
	RetentionMS int `json:"logstore_retention_ms,omitempty" example:"604800000"`
}

func (c *StreamStorageConfig) AppendToParams(params *url.Values) {
	params.Add("logstore_retention_bytes", fmt.Sprintf("%d", c.RetentionBytes))
	params.Add("logstore_retention_ms", fmt.Sprintf("%d", c.RetentionMS))
}

type IngestData struct {
	Columns []string `json:"columns"`
	Data    [][]any  `json:"data"`
}

// NewClient creates proton client. It includes both rest and proton-go client.
func NewClient(host string, port int, restUser, restPassword string) *Client {
	httpClient := utils.NewDefaultHttpClient()
	return &Client{
		host:         host,
		baseURL:      fmt.Sprintf("http://%s:%d", host, port),
		restUser:     restUser,
		restPassword: restPassword,
		client:       httpClient,
	}
}

func (s *Client) buildVersionedURL(url string) string {
	return fmt.Sprintf("%s/proton/v1/%s", s.baseURL, url)
}

// request will propragate error if the response code is not 2XX, payload will be marshaled as JSON format
func (s *Client) request(method string, path string, payload any) ([]byte, error) {
	var body []byte
	if payload == nil {
		log.Logger().Debugf("send empty request to url %s", path)
	} else {
		jsonPostValue, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("invalid payload: %w", err)
		}
		body = jsonPostValue
	}

	return s.rawRequest(method, path, body)
}

func retryOnBrokenIdleConnection(action func() error) (err error) {
	for i := 0; i < retries; i++ {
		err = action()
		if err == nil {
			break
		}
		if err, ok := err.(*url.Error); ok {
			msg := err.Err.Error()
			// only retry "EOF" which (usually) means the Proton connection can't be used any more
			if msg == "EOF" || msg == "http: server closed idle connection" {
				continue
			}
		}
		break
	}
	return err
}

// HTTPStatusCode = -1 indicates the error is not from a HTTP request (e.g. from TCP)
type ErrorResponse struct {
	HTTPStatusCode int
	// proton error codes: https://github.com/timeplus-io/proton/blob/develop/src/Common/ErrorCodes.cpp
	Code      int    `json:"code"`
	RequestId string `json:"request_id"`
	Message   string `json:"error_msg"`
}

func (e *ErrorResponse) Error() string {
	if e.Code > 0 {
		return fmt.Sprintf("code: %d, message: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("response code: %d, message: %s", e.HTTPStatusCode, e.Message)
}

func UnmarshalErrorResponse(resp *http.Response) error {
	if resp.StatusCode > 199 && resp.StatusCode < 300 {
		return nil
	}

	e := &ErrorResponse{
		HTTPStatusCode: resp.StatusCode,
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e.Message = err.Error()
		return e
	}

	if json.Unmarshal(body, e) != nil {
		e.Message = string(body)
	}

	return e
}

func (s *Client) rawRequest(method string, path string, body []byte) ([]byte, error) {
	var res *http.Response
	err := retryOnBrokenIdleConnection(func() error {
		req, err := http.NewRequest(method, path, bytes.NewBuffer(body))
		if err != nil {
			return err
		}
		req.SetBasicAuth(s.restUser, s.restPassword)
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")

		resp, err := s.client.Do(req)
		if err == nil {
			res = resp
		}
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrProtonDown, err.Error())
	}

	if err := UnmarshalErrorResponse(res); err != nil {
		return nil, err
	}

	defer res.Body.Close()
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return resBody, nil
}

func (s *Client) CreateStream(stream StreamDef, streamStorageConfig StreamStorageConfig) error {
	log.Logger().Infof("creating stream %v", stream)

	base, err := url.Parse(s.buildVersionedURL(StreamsPath))
	if err != nil {
		return err
	}

	// Query params
	params := url.Values{}
	streamStorageConfig.AppendToParams(&params)
	base.RawQuery = params.Encode()

	if _, err := s.request(http.MethodPost, base.String(), stream); err != nil {
		return err
	}

	log.Logger().Info("stream created")
	return nil
}

func IsUnknownStreamError(err error) bool {
	var protonErr *ErrorResponse

	if errors.As(err, &protonErr) {

		if protonErr.Code == 60 {
			return true
		}
	}

	return false
}

func (s *Client) DeleteStream(name string) error {
	url := s.buildVersionedURL(fmt.Sprintf("%s/%s", StreamsPath, name))

	if _, err := s.request(http.MethodDelete, url, nil); err != nil {
		if !IsUnknownStreamError(err) {
			return err
		}
	}

	return nil
}

func (s *Client) InsertData(data IngestData, stream string) (int, error) {

	url := s.buildVersionedURL(fmt.Sprintf("%s/%s", IngestPath, stream))

	jsonPostValue, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal ingest data: %w", err)
	}

	if _, err := s.rawRequest(http.MethodPost, url, jsonPostValue); err != nil {
		return 0, err
	}

	return len(jsonPostValue), nil
}
