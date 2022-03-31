package proton

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/spf13/viper"

	"github.com/timeplus-io/chameleon/generator/log"
)

const TIME_FORMAT = "2006-01-02 15:04:05.000"

// versioned endpoints
// example: http://host:port/proton/v1/ddl/streams
const CREATE_STREAM_PATH = "ddl/streams"
const INTEST_PATH = "ingest/streams"
const FORMAT_SQL_PATH = "sqlformat"
const SEARCH_PATH = "search"

type TimestampIntType int64

type ProtonServer struct {
	host     string
	user     string
	password string
	baseURL  string
	client   *http.Client
}

type ColumnDef struct {
	Name                    string `json:"name"`
	Type                    string `json:"type"`
	Default                 string `json:"default,omitempty"`
	Nullable                bool   `json:"nullable,omitempty"`
	CompressionCodec        string `json:"compression_codec,omitempty"`
	TTLExpression           string `json:"ttl_expression,omitempty"`
	SkippingIndexExpression string `json:"skipping_index_expression,omitempty"`
}

type StreamDef struct {
	Name                   string      `json:"name"`
	Columns                []ColumnDef `json:"columns"`
	EventTimeColumn        string      `json:"event_time_column,omitempty"`
	Shards                 int         `json:"shards,omitempty"`
	ReplicationFactor      int         `json:"replication_factor,omitempty"`
	OrderByExpression      string      `json:"order_by_expression,omitempty"`
	OrderByGranularity     string      `json:"order_by_granularity,omitempty"`
	PartitionByGranularity string      `json:"partition_by_granularity,omitempty"`
	TTLExpression          string      `json:"ttl_expression,omitempty"`
}

type StreamStorageConfig struct {
	RetentionBytes int `json:"streaming_storage_retention_bytes,omitempty"`
	RetentionMS    int `json:"streaming_storage_retention_ms,omitempty"`
}

type GetStreamResp struct {
	Name         string        `json:"name"`
	Engine       string        `json:"engine"`
	ORDER_BY     string        `json:"order_by_expression"`
	PATTITION_BY string        `json:"partition_by_expression"`
	TTL          string        `json:"ttl"`
	COLUMNS      []ColumnsResp `json:"columns"`
}

type ColumnsResp struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Default  string `json:"default"`
	Alias    string `json:"alias"`
	Comment  string `json:"comment"`
	Codec    string `json:"codec"`
	TTL      string `json:"ttl"`
}

type GetStreamsResp []GetStreamResp

type GetStreamsEnvelopResp struct {
	Data GetStreamsResp `json:"data"`
}

func NewStreamStorageConfig() *StreamStorageConfig {
	return &StreamStorageConfig{
		RetentionBytes: 0,
		RetentionMS:    0,
	}
}

func (c *StreamStorageConfig) SetRetentionBytes(retentionBytes int) {
	c.RetentionBytes = retentionBytes
}

func (c *StreamStorageConfig) SetRetentionMS(retentionMS int) {
	c.RetentionMS = retentionMS
}

func NewDefaultServer(httpClient *http.Client) *ProtonServer {
	host := viper.GetString("proton-addr")
	username := viper.GetString("proton-username")
	password := viper.GetString("proton-password")
	return newServer(host, username, password, httpClient)
}

func NewDefaultHttpClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = viper.GetInt("poroton-http-max-idle-connection")
	t.MaxConnsPerHost = viper.GetInt("poroton-http-max-connection-per-host")
	t.MaxIdleConnsPerHost = viper.GetInt("poroton-http-max-idle-connection-per-host")

	return &http.Client{
		Timeout:   viper.GetDuration("poroton-http-timeout") * time.Second,
		Transport: t,
	}
}

func newServer(host, user, password string, httpClient *http.Client) *ProtonServer {
	server := ProtonServer{
		host:     host,
		baseURL:  fmt.Sprintf("http://%s:%d", host, viper.GetInt("proton-http-port")),
		user:     user,
		password: password,
		client:   httpClient,
	}

	return &server
}

// request will propragate error if the response code is not 2XX
func (s *ProtonServer) request(method string, url string, payload interface{}) (int, []byte, error) {
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
	req.SetBasicAuth(s.user, s.password)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	res, err := s.client.Do(req)
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

func (s *ProtonServer) CreateStream(stream StreamDef, streamStorageConfig StreamStorageConfig) error {
	log.Logger().Infof("creating stream %v", stream)

	base, err := url.Parse(s.buildVersionedURL(CREATE_STREAM_PATH))
	if err != nil {
		return nil
	}

	// Query params
	params := url.Values{}

	if streamStorageConfig.RetentionBytes != 0 {
		params.Add("streaming_storage_retention_bytes", fmt.Sprintf("%d", streamStorageConfig.RetentionBytes))
	}

	if streamStorageConfig.RetentionMS != 0 {
		params.Add("streaming_storage_retention_ms", fmt.Sprintf("%d", streamStorageConfig.RetentionMS))
	}
	base.RawQuery = params.Encode()

	if _, _, err := s.request(http.MethodPost, base.String(), stream); err != nil {
		return fmt.Errorf("failed to create stream %v: %s", stream, err)
	}

	log.Logger().Info("stream created")

	return nil
}

func (s *ProtonServer) ListStreams() (*GetStreamsEnvelopResp, error) {
	url := s.buildVersionedURL(CREATE_STREAM_PATH)
	if _, respBody, err := s.request(http.MethodGet, url, nil); err != nil {
		return nil, fmt.Errorf("failed to list streams %w", err)
	} else {
		var payload GetStreamsEnvelopResp
		json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)
		return &payload, nil
	}
}

func (s *ProtonServer) ListStreamNames() ([]string, error) {
	resp, err := s.ListStreams()

	if err != nil {
		return []string{}, err
	}

	names := make([]string, len(resp.Data))

	for i, data := range resp.Data {
		names[i] = data.Name
	}
	return names, nil
}

func (s *ProtonServer) StreamExist(name string) (bool, error) {
	streamNames, err := s.ListStreamNames()
	if err != nil {
		return false, err
	}

	for _, item := range streamNames {
		if item == name {
			return true, nil
		}
	}
	return false, nil
}

func (s *ProtonServer) DeleteStream(stream string) error {
	log.Logger().Infof("Delete stream %s", stream)
	url := s.buildVersionedURL(fmt.Sprintf("%s/%s", CREATE_STREAM_PATH, stream))
	if _, _, err := s.request(http.MethodDelete, url, nil); err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", stream, err)
	}

	return nil
}

func (s *ProtonServer) processingDatetime(data IngestData) IngestData {
	result := data
	for i, d := range result.Data {
		for j, cell := range d {
			switch cell.(type) {
			case time.Time:
				result.Data[i][j] = cell.(time.Time).Format(TIME_FORMAT)
			default:
			}
		}
	}

	return result
}

func (s *ProtonServer) Ingest(data IngestData, stream string) error {
	processedData := s.processingDatetime(data)

	url := s.buildVersionedURL(fmt.Sprintf("%s/%s", INTEST_PATH, stream))

	_, _, err := s.request(http.MethodPost, url, processedData)
	if err != nil {
		return fmt.Errorf("failed to ingest data to stream %s: %w", stream, err)
	}
	return nil
}

func (s *ProtonServer) buildVersionedURL(url string) string {
	return fmt.Sprintf("%s/proton/v1/%s", s.baseURL, url)
}
