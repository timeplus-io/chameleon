package proton

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/cardemo/log"
)

const timeFormat = "2006-01-02 15:04:05.000"
const streamDefaultNamespace = "default"

const (
	// versioned endpoints
	// example: http://host:port/proton/v1/ddl/streams
	StreamsPath     = "ddl/streams"
	IngestPath      = "ingest/streams"
	FormatSQLPath   = "sqlformat"
	AnalyzeSQLPath  = "sqlanalyzer"
	PipelinePath    = "pipeline_metrics"
	SearchPath      = "search"
	UdfPath         = "udfs"
	StorageInfoPath = "storageinfo"

	// unversioned endpoints
	// example: http://host:port/proton/ping
	PingPath = "ping"
	InfoPath = "info"
)

type TimestampIntType int64

type Client struct {
	host         string
	restUser     string
	restPassword string
	baseURL      string
	client       *http.Client
	systemDriver engine
	userDriver   engine
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

type ViewDef struct {
	Name         string
	Query        string
	Materialized bool
	TargetStream string
}

type UDFDef struct {
	// Either `javascript` or `remote`
	Type string `json:"type" binding:"required" example:"remote"`
	Name string `json:"name" binding:"required" example:"sum_2"`

	// The input argument of the UDF
	//   * For UDA: the number and type of arguments should be consistent with the main function of UDA.
	//     the type should be the data types of proton not javascript types. It only supports int8/16/32/64, uint8/16/32/64,
	Arguments []UDFArgument `json:"arguments"`

	// The erturn type of the UDF
	//   * For UDA: if it returns a single value, the return type is the corresponding data type of Timeplus.
	//     It supports the same types of input arguments, except for datetime, it only supports DateTime64(3).
	ReturnType string `json:"return_type" example:"float64"`

	// Only valid when `type` is `remote`.
	URL string `json:"url,omitempty" example:"http://mydomain.com/trigger"`

	// Only valid when `type` is `remote`.
	// This field is used to set the authentication method for remote UDF. It can be either `auth_header` or `none`.
	// When `auth_header` is set, you can configure `auth_context` to specify the HTTP header that be sent the remote URL
	AuthMethod string `json:"auth_method,omitempty"`

	// Only valid when `type` is `remote` and `auth_method` is `auth_header`
	AuthContext *UDFAuthContext `json:"auth_context,omitempty"`

	// Only valid when type is 'javascript'. Whether it is an aggregation function.
	IsAggrFunction bool `json:"is_aggregation,omitempty"`

	// Only valid when type is 'javascript'
	// The source code of the UDA. There are functions to be defined:
	//  * main function: with the same name as UDA. Timeplus calls this function for each input row. The main function can return two types of result: object or simple data type
	//    - If it returns an object, the object is like {“emit”: true, “result”: …}. ‘Emit’ (boolean) property tells Timeplus whether or not the result should emit. ‘result’ is the current aggregate result, if ‘emit’ is false, the result will be ignored by Timeplus. Timeplus will convert the ‘result’ property of v8 to the data types defined when creating UDA.
	//    - If it returns a simple data type, Timeplus considers the return data as the result to be emitted immediately. It converts the return data to the corresponding data type and Timeplus emits the aggregating result.
	//    - Once UDA tells Timeplus to emit the data, UDA takes the full responsibility to clear the internal state, prepare and restart a new aggregating window, et al.
	//  * state function: which returns the serialized state of all internal states of UDA in string. The UDA takes the responsibility therefore Timeplus can choose to persist the internal state of UDA for query recovery.
	//  * init function: the input of this function is the string of serialized state of the internal states UDA. Timeplus calls this function when it wants to recover the aggregation function with the persisted internal state.
	Source string `json:"source,omitempty"`
} // @name UDFDef

type UDFArgument struct {
	Name string `json:"name" binding:"required" example:"val"`
	Type string `json:"type" binding:"required" example:"float64"`
} // @name UDFArgument

type UDFAuthContext struct {
	Name  string `json:"key_name"`
	Value string `json:"key_value"`
} // @name UDFAuthContext

type GetUDFsResp struct {
	Data      []UDFDef `json:"data"`
	RequestId string   `json:"request_id"`
} // @name GetUDFsResp

type GetUDFResp struct {
	Data      UDFDef `json:"data"`
	RequestId string `json:"request_id"`
} // @name GetUDFResp

type StreamSetting struct {
	Key   string `json:"key" mapstructure:"key"`
	Value string `json:"value" mapstructure:"value"`
} // @name StreamSetting

type ExternalStreamDef struct {
	Name     string          `json:"name" binding:"required" example:"external_kafka"`
	Settings []StreamSetting `json:"settings"`
} // @name ExternalStreamDef

type SQLAnalyzeResult struct {
	HasAggregation  bool               `json:"has_aggr"`
	HasSubquery     bool               `json:"has_subquery"`
	HasTableJoin    bool               `json:"has_table_join"`
	HasUnion        bool               `json:"has_union"`
	IsStreaming     bool               `json:"is_streaming"`
	OriginalQuery   string             `json:"original_query"`
	QueryType       string             `json:"query_type"`
	RequiredColumns []SQLAnalyzeColumn `json:"required_columns"`
	ResultColumns   []SQLAnalyzeColumn `json:"result_columns"`
	RewrittenQuery  string             `json:"rewritten_query"`
	GroupByColumns  []string           `json:"group_by_columns"`
} // @name SQLAnalyzeResult

type SQLAnalyzeColumn struct {
	Column     string `json:"column"`
	ColumnType string `json:"column_type"`
	Database   string `json:"database"`
	IsView     bool   `json:"is_view"`
	Table      string `json:"table"`
} // @name SQLAnalyzeColumn

type GetStreamResp struct {
	Name   string `json:"name" binding:"required" example:"test_stream"`
	Engine string `json:"engine" binding:"required" example:"Stream"`
	//ORDER_BY     string        `json:"order_by_expression"`
	//PATTITION_BY string        `json:"partition_by_expression"`
	TTLExpression string `json:"ttl_expression" binding:"required" example:"to_datetime(_tp_time) + INTERVAL 7 DAY"`

	// Storage mode of stream. Defaulted to `append`.
	Mode string `json:"mode"  binding:"required" enums:"append,changelog,changelog_kv,versioned_kv"`

	// Expression of primary key, required in `changelog_kv` and `versioned_kv` mode
	PrimaryKey string        `json:"primary_key,omitempty"`
	Columns    []ColumnsResp `json:"columns" binding:"required"`

	// The max size a stream can grow. Any non-positive value means unlimited size.
	RetentionBytes int `json:"logstore_retention_bytes" binding:"required" example:"1073741824"`

	// The max time the data can be retained in the stream. Any non-positive value means unlimited time.
	RetentionMS int `json:"logstore_retention_ms" binding:"required" example:"86400000"`

	// Deprecated. Use `ttl_expression` instaed
	TTL string `json:"ttl" binding:"required" example:"to_datetime(_tp_time) + INTERVAL 7 DAY"`
}

type GetViewResp struct {
	Name           string
	Query          string
	Materialized   bool
	Columns        []ColumnsResp
	TTL            string
	RetentionBytes int
	RetentionMS    int
}

type ColumnsResp struct {
	Name     string `json:"name" example:"my_col"`
	Type     string `json:"type" example:"int64"`
	Nullable bool   `json:"nullable"`
	Default  string `json:"default"`
	Codec    string `json:"codec" example:"CODEC(ZSTD(1))"`

	// Alias    string `json:"alias"`
	// Comment  string `json:"comment"`
	//TTL      string `json:"ttl"`
} // @name ColumnsResp

type GetStreamsEnvelopResp struct {
	Data []GetStreamResp `json:"data"`
}

type storageInfoEnvelopResp struct {
	Data storageInfoResp `json:"data"`
}

type storageInfoResp struct {
	TotalBytesOnDisk int64                        `json:"total_bytes_on_disk"`
	Streams          map[string]StreamStroageInfo `json:"streams"`
}

type StreamStroageInfo struct {
	StreamingDataBytes  int `json:"streaming_data_bytes" example:"12345"`
	HistoricalDataBytes int `json:"historical_data_bytes" example:"234567"`
}

type QueryReq struct {
	Query string `json:"query"`
}

type QueryStat struct {
	Elasped   float32 `json:"elapsed"`
	RowsRead  int     `json:"rows_read"`
	BytesRead int     `json:"bytes_read"`
}

type FormatQueryReq struct {
	SQL string `json:"query"`
}

type FormatQueryResp struct {
	SQL string `json:"query"`
}

type GetQueryPipelineReq struct {
	ID string `json:"query_id"`
}

type GetQueryPipelineResp struct {
	Edges []PipelineEdge `json:"edges"`
	Nodes []PipelineNode `json:"nodes"`
} // @name QueryPipeline

type PipelineEdge struct {
	From int `json:"from"`
	To   int `json:"to"`
} // @name QueryPipelineEdge

type PipelineNode struct {
	ID     int                `json:"id"`
	Metric PipelineNodeMetric `json:"metric"`
	Name   string             `json:"name"`
	Status string             `json:"status"`
} // @name QueryPipelineNode

type PipelineNodeMetric struct {
	ProcessedBytes  int64 `json:"processed_bytes"`
	ProcessedTimeNS int64 `json:"processing_time_ns"`
} // @name QueryPipelineNodeMetric

type InfoResp struct {
	Build BuildInfo `json:"build"`
}

type BuildInfo struct {
	Name    string `json:"name"`
	Time    string `json:"time"`
	Version string `json:"version"`
	Commit  string `json:"commit_sha"`
}

type StreamStats struct {
	RowCount      uint64     `json:"row_count" example:"20"`
	EarliestEvent *time.Time `json:"earliest_event,omitempty" example:"2023-02-01T01:02:03.456Z"`
	LatestEvent   *time.Time `json:"latest_event,omitempty" example:"2023-02-13T07:08:09.012Z"`
	StreamStroageInfo
} // @name StreamStats

type streamUpdateReq struct {
	*StreamStorageConfig
	TTLExpression *string `json:"ttl_expression,omitempty"`
}

// NewClient creates proton client. It includes both rest and proton-go client.
func NewClient(tenant, host string, port int, restUser, restPassword string, systemDriver, userDriver engine, ops ...HTTPOption) *Client {
	httpClient := newHTTPClient(ops...)
	return &Client{
		host:         host,
		baseURL:      fmt.Sprintf("http://%s:%d", host, port),
		restUser:     restUser,
		restPassword: restPassword,
		client:       httpClient,
		systemDriver: systemDriver,
		userDriver:   userDriver,
	}
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

// TODO: use ExecWithParams
func (s *Client) CreateExternalStream(stream ExternalStreamDef) error {
	settings := make([]string, len(stream.Settings))
	for index, setting := range stream.Settings {
		settings[index] = fmt.Sprintf("%s='%s'", setting.Key, setting.Value)
	}

	// Hardcoded column name to be `raw` and type to be `string`
	createSQL := fmt.Sprintf("CREATE EXTERNAL STREAM %s(%s %s) SETTINGS %s", stream.Name, "raw", "string", strings.Join(settings, ","))
	return s.systemDriver.ExecWithParams(createSQL)
}

func (s *Client) CreateView(view ViewDef) error {
	var (
		viewType string
		into     string
	)
	if view.Materialized {
		viewType = "MATERIALIZED VIEW"
		if len(strings.TrimSpace(view.TargetStream)) > 0 {
			into = fmt.Sprintf("INTO `%s`", strings.TrimSpace(view.TargetStream))
		}
	} else {
		viewType = "VIEW"
	}

	createSQL := fmt.Sprintf("CREATE %s `%s` %s AS %s", viewType, view.Name, into, view.Query)
	return s.systemDriver.ExecWithParams(createSQL)
}

func (s *Client) ListStreams() ([]GetStreamResp, error) {
	url := s.buildVersionedURL(StreamsPath)
	respBody, err := s.request(http.MethodGet, url, nil)
	if err != nil {
		if IsEmptyResult(err) {
			return make([]GetStreamResp, 0), nil
		}

		return nil, err
	}

	result, err := unmarshalGetStreamsEnvelopResp(respBody)
	if err != nil {
		return nil, err
	}

	return result.Data, nil
}

func (s *Client) ExistStream(name string) bool {
	_, err := s.GetStream(name)
	if err != nil {
		return false
	}
	return true
}

func (s *Client) ExistView(name string) bool {
	_, err := s.GetView(name)
	if err != nil {
		return false
	}
	return true
}

func (s *Client) GetStream(name string) (*GetStreamResp, error) {
	url := fmt.Sprintf("%s/default/%s", s.buildVersionedURL(StreamsPath), name)
	respBody, err := s.request(http.MethodGet, url, nil)
	if err != nil {
		if IsEmptyResult(err) {
			return nil, nil
		}

		return nil, err
	}

	result, err := unmarshalGetStreamsEnvelopResp(respBody)
	if err != nil {
		return nil, err
	}

	if len(result.Data) != 1 {
		return nil, fmt.Errorf("got wrong number of streams %d", len(result.Data))
	}

	stream := result.Data[0]

	return &stream, nil
}

func (s *Client) GetStreamStats(name string) (*StreamStats, error) {
	stats := StreamStats{}

	storageInfos, rowCounts := s.getStreamsStats()

	if count, exists := rowCounts[name]; exists {
		stats.RowCount = count
	}
	if storageInfo, exists := storageInfos[name]; exists {
		stats.StreamStroageInfo = storageInfo
	}
	if stats.RowCount > 0 {
		if earliest, latest, err := s.getStreamEvents(name); err == nil {
			stats.EarliestEvent = earliest
			stats.LatestEvent = latest
		}
	}

	return &stats, nil
}

// TODO: Need to wait Proton to have a consolidated list stream/view endpoint
// List view is very weird. We need to join multiple sources together to get the full picture of a view
//  1. proton/v1/ddl/streams: for columns and retention policy
//  2. select X from system.tables: for query and uuid
//  3. storageinfo: use uuid to join storage info
func (s *Client) ListViews() ([]GetViewResp, error) {
	url := s.buildVersionedURL(StreamsPath)
	respBody, err := s.request(http.MethodGet, url, nil)
	if err != nil {
		if IsEmptyResult(err) {
			return make([]GetViewResp, 0), nil
		}

		return nil, err
	}

	streams, err := unmarshalGetStreamsEnvelopResp(respBody)
	if err != nil {
		return nil, err
	}

	_, events, err := s.systemDriver.SyncQuery("select name, create_table_query from system.tables where database == 'default' and engine like '%View%'", 3*time.Second)
	if err != nil {
		return nil, err
	}

	result := make([]GetViewResp, 0)
	for _, event := range events {
		view := new(GetViewResp)
		var OK bool

		view.Name, OK = event[0].(string)
		if !OK {
			return nil, fmt.Errorf("failed to get the name of view (%v)", event[0])
		}

		view.Query, OK = event[1].(string)
		if !OK {
			return nil, fmt.Errorf("failed to get the query of view (%v)", event[1])
		}

		for _, data := range streams.Data {
			if data.Name == view.Name {
				view.Columns = data.Columns
				view.RetentionBytes = data.RetentionBytes
				view.TTL = data.TTLExpression
				view.RetentionMS = data.RetentionMS
				if data.Engine == "MaterializedView" {
					view.Materialized = true
				}
			}
		}

		result = append(result, *view)
	}

	return result, nil
}

func (s *Client) GetView(name string) (*GetViewResp, error) {
	stream, err := s.GetStream(name)
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, nil
	}

	_, events, err := s.systemDriver.SyncQuery(fmt.Sprintf("select create_table_query from system.tables where database == 'default' and name=='%s'", name), 3*time.Second)
	if err != nil {
		return nil, err
	}

	if len(events) != 1 {
		return nil, fmt.Errorf("got unexpected number of events: %d", len(events))
	}

	event := events[0]

	var (
		view = new(GetViewResp)
		OK   bool
	)

	view.Name = name
	view.Query, OK = event[0].(string)
	if !OK {
		return nil, fmt.Errorf("failed to get the query of view (%v)", event[1])
	}

	if stream.Name == view.Name {
		view.Columns = stream.Columns
		view.TTL = stream.TTLExpression
		view.RetentionBytes = stream.RetentionBytes
		view.RetentionMS = stream.RetentionMS
		if stream.Engine == "MaterializedView" {
			view.Materialized = true
		}
	}

	return view, nil
}

// Only for materialized view
func (s *Client) GetViewStats(name string) (*StreamStats, error) {
	_, events, err := s.systemDriver.SyncQuery(fmt.Sprintf("select uuid from system.tables where database == 'default' and name=='%s'", name), 3*time.Second)
	if err != nil {
		return nil, err
	}

	if len(events) != 1 {
		return nil, fmt.Errorf("got unexpected number of events: %d", len(events))
	}

	event := events[0]
	id, OK := event[0].(string)
	if !OK {
		return nil, fmt.Errorf("failed to get uuid: %v", event)
	}

	stats := StreamStats{}

	storageInfos, rowCounts := s.getStreamsStats()

	for name, info := range storageInfos {
		// A sample mview id: .inner.target-id.7515ee5f-5bbe-4387-83d8-633744c400df
		if strings.Contains(name, id) {
			stats.StreamStroageInfo = info
			break
		}
	}

	for name, count := range rowCounts {
		if strings.Contains(name, id) {
			stats.RowCount = count
			break
		}
	}

	if stats.RowCount > 0 {
		if earliest, latest, err := s.getStreamEvents(name); err == nil {
			stats.EarliestEvent = earliest
			stats.LatestEvent = latest
		}
	}

	return &stats, nil
}

func (s *Client) AnalyzeSQL(sql string) (*SQLAnalyzeResult, error) {
	log.Logger().Debugf("Analyze Table with %s", sql)
	url := s.buildVersionedURL(AnalyzeSQLPath)

	request := QueryReq{
		Query: sql,
	}

	respBody, err := s.request(http.MethodPost, url, request)
	if err != nil {
		return nil, err
	}

	result := new(SQLAnalyzeResult)
	if err := json.Unmarshal(respBody, result); err != nil {
		return nil, fmt.Errorf("failed to decode analyze response: %w", err)
	}

	return result, nil
}

func (s *Client) DeleteStreamView(name string) error {
	url := s.buildVersionedURL(fmt.Sprintf("%s/%s", StreamsPath, name))

	if _, err := s.request(http.MethodDelete, url, nil); err != nil {
		if !IsUnknownStreamError(err) {
			return err
		}
	}

	return nil
}

func (s *Client) UpdateStream(name string, ttl *string, streamStorageConfig *StreamStorageConfig) error {
	return s.updateTTLRetention(name, ttl, streamStorageConfig)
}

func (s *Client) UpdateView(name string, ttl *string, streamStorageConfig *StreamStorageConfig) error {
	return s.updateTTLRetention(name, ttl, streamStorageConfig)
}

func (s *Client) updateTTLRetention(name string, ttl *string, streamStorageConfig *StreamStorageConfig) error {
	if ttl == nil && streamStorageConfig == nil {
		return nil
	}

	req := streamUpdateReq{
		StreamStorageConfig: streamStorageConfig,
		TTLExpression:       ttl,
	}

	url := s.buildVersionedURL(fmt.Sprintf("%s/%s", StreamsPath, name))
	if _, err := s.request(http.MethodPatch, url, req); err != nil {
		return err
	}

	return nil
}

func (s *Client) processingDatetime(data IngestData) IngestData {
	result := data
	for i, d := range result.Data {
		for j, cell := range d {
			switch cell := cell.(type) {
			case time.Time:
				result.Data[i][j] = cell.Format(timeFormat)
			default:
			}
		}
	}

	return result
}

func (s *Client) IngestRaw(payload []byte, stream string) (int, error) {
	url := s.buildVersionedURL(fmt.Sprintf("%s/%s", IngestPath, stream))

	if _, err := s.rawRequest(http.MethodPost, url, payload); err != nil {
		return 0, err
	}

	return len(payload), nil
}

func (s *Client) IngestEvent(data IngestData, stream string) (int, error) {
	return s.ingestEvent(data, stream, true)
}

func (s *Client) IngestEventAsIs(data IngestData, stream string) (int, error) {
	return s.ingestEvent(data, stream, false)
}

func (s *Client) ingestEvent(data IngestData, stream string, formatDatetime bool) (int, error) {
	if formatDatetime {
		// https://github.com/timeplus-io/neutron/internal/issues/295
		// process datetime before sending to other routes
		data = s.processingDatetime(data)
	}
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

func (s *Client) Info() (*InfoResp, error) {
	url := s.buildURL(InfoPath)
	if respBody, err := s.request(http.MethodGet, url, nil); err != nil {
		return nil, err
	} else {
		payload := new(InfoResp)
		if err := json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		return payload, nil
	}
}

// Ping HTTP endpoint first, then TCP
func (s *Client) Ping() error {
	url := s.buildURL(PingPath)
	if _, err := s.request(http.MethodGet, url, nil); err != nil {
		return err
	}

	return s.systemDriver.Ping()
}

func (s *Client) QueryStream(ctx context.Context, startSign <-chan struct{}, sql, id string) ([]Column, rxgo.Observable, rxgo.Observable, error) {
	return s.userDriver.QueryStream(ctx, startSign, sql, id)
}

// We HAVE TO user system driver to stop/usubscribe query since if the user hit the concurrent queries limits
// they won't even be able to spawn another query to stop them.
func (s *Client) StopQuery(id string) {
	s.systemDriver.StopQuery(id)
}

func (s *Client) FormatQuery(sql string) (*FormatQueryResp, error) {
	url := s.buildVersionedURL(FormatSQLPath)

	reqBody := FormatQueryReq{
		SQL: sql,
	}

	respBody, err := s.request(http.MethodPost, url, reqBody)
	if err != nil {
		return nil, err
	}

	result := new(FormatQueryResp)
	if err := json.Unmarshal(respBody, result); err != nil {
		return nil, fmt.Errorf("failed to decode format response: %w", err)
	}

	return result, nil
}

func (s *Client) GetQueryPipeline(id string) (*GetQueryPipelineResp, error) {
	url := s.buildVersionedURL(PipelinePath)

	reqBody := GetQueryPipelineReq{
		ID: id,
	}

	respBody, err := s.request(http.MethodPost, url, reqBody)
	if err != nil {
		return nil, err
	}

	result := new(GetQueryPipelineResp)
	if err := json.Unmarshal(respBody, result); err != nil {
		return nil, fmt.Errorf("failed to decode into response: %w", err)
	}

	return result, nil
}

func (s *Client) UpsertUDF(udf UDFDef) error {
	base, err := url.Parse(s.buildVersionedURL(UdfPath))
	if err != nil {
		return err
	}

	if _, err := s.request(http.MethodPost, base.String(), udf); err != nil {
		return err
	}

	return nil
}

func (s *Client) ListUDF() (*GetUDFsResp, error) {
	log.Logger().Debugf("listing udf")

	base, err := url.Parse(s.buildVersionedURL(UdfPath))
	if err != nil {
		return nil, err
	}

	respBody, err := s.request(http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, err
	}

	result := new(GetUDFsResp)
	if err := json.Unmarshal(respBody, result); err != nil {
		return nil, fmt.Errorf("failed to decode list udf response: %w", err)
	}

	return result, nil
}

func (s *Client) GetUDF(name string) (*GetUDFResp, error) {
	log.Logger().Debugf("getting udf")

	base, err := url.Parse(s.buildVersionedURL(fmt.Sprintf("%s/%s", UdfPath, name)))
	if err != nil {
		return nil, err
	}

	respBody, err := s.request(http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, err
	}

	result := new(GetUDFResp)
	if err := json.Unmarshal(respBody, result); err != nil {
		return nil, fmt.Errorf("failed to decode get udf response: %w", err)
	}

	return result, nil
}

func (s *Client) DeleteUDF(name string) error {
	log.Logger().Debugf("deleting udf")

	base, err := url.Parse(s.buildVersionedURL(fmt.Sprintf("%s/%s", UdfPath, name)))
	if err != nil {
		return err
	}

	if _, err := s.request(http.MethodDelete, base.String(), nil); err != nil {
		return err
	}

	return nil
}

func (s *Client) buildURL(url string) string {
	return fmt.Sprintf("%s/proton/%s", s.baseURL, url)
}

func (s *Client) buildVersionedURL(url string) string {
	return fmt.Sprintf("%s/proton/v1/%s", s.baseURL, url)
}

// For testing oly
func (s *Client) GetClient() *http.Client {
	return s.client
}

func (s *Client) GetTotalStorage() (int64, error) {
	url := s.buildVersionedURL(StorageInfoPath)
	respBody, err := s.request(http.MethodGet, url, nil)
	if err != nil {
		return -1, err
	}
	storage := new(storageInfoEnvelopResp)
	if err := json.Unmarshal(respBody, storage); err != nil {
		return -1, err
	}

	return storage.Data.TotalBytesOnDisk, nil
}

func (s *Client) getStreamsStorage() (map[string]StreamStroageInfo, error) {
	storageInfos := make(map[string]StreamStroageInfo)

	url := s.buildVersionedURL(StorageInfoPath)
	respBody, err := s.request(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	storage := new(storageInfoEnvelopResp)
	if err := json.Unmarshal(respBody, storage); err != nil {
		return nil, err
	}
	prefix := streamDefaultNamespace + "."
	for k, storageInfo := range storage.Data.Streams {

		if !strings.HasPrefix(k, prefix) {
			continue
		}

		// default.iot -> iot
		streamName := k[len(prefix):]

		// default.`special-name` -> special-name
		if strings.HasPrefix(streamName, "`") && strings.HasSuffix(streamName, "`") {
			streamName = streamName[1 : len(streamName)-1]
		}
		storageInfos[streamName] = storageInfo
	}

	return storageInfos, nil
}

func (s *Client) getStreamsStats() (map[string]StreamStroageInfo, map[string]uint64) {
	logger := log.Logger()
	rowCounts := make(map[string]uint64)

	storageInfos, err := s.getStreamsStorage()
	if err != nil {
		logger.WithError(err).Warn("failed to get storage info for streams")
		storageInfos = make(map[string]StreamStroageInfo)
	}

	if _, events, err := s.systemDriver.SyncQuery("SELECT name, total_rows FROM system.tables", 3*time.Second); err != nil {
		logger.WithError(err).Warn("failed to get stats for streams")
	} else {
		for _, event := range events {
			name, OK := event[0].(string)
			if !OK {
				logger.Warnf("stream name is not available: %v", event[0])
				continue
			}
			count, OK := event[1].(*uint64)
			if !OK || count == nil {
				// External stream has `count == nil`
				logger.WithField("name", name).Debugf("stream row count is not available: %v", event[1])
				continue
			}

			rowCounts[name] = *count
		}
	}

	return storageInfos, rowCounts
}

func (s *Client) getStreamEvents(name string) (*time.Time, *time.Time, error) {
	_, rows, err := s.systemDriver.SyncQuery(fmt.Sprintf("select min(_tp_time) as earliest,max(_tp_time) as latest from table(%s)", name), 3*time.Second)
	if err != nil {
		return nil, nil, err
	}

	if len(rows) != 1 {
		return nil, nil, errors.New("received more than 1 rows")
	}

	row := rows[0]

	earliest, ok := row[0].(time.Time)
	if !ok {
		return nil, nil, errors.New("earliest is not a valid time")
	}
	latest, ok := row[1].(time.Time)
	if !ok {
		return nil, nil, errors.New("latest is not a valid time")
	}

	return &earliest, &latest, nil
}

// Similar to IsEmptyResult, delete a non-existing stream/view returns 500 with code 60
func IsUnknownStreamError(err error) bool {
	var protonErr *ErrorResponse

	if errors.As(err, &protonErr) {

		if protonErr.Code == 60 {
			return true
		}
	}

	return false
}

// Known Proton issue: For list stream/view or get streams/:stream, views/:view endpoints, when there is no stream/view, Proton returns
// 500 with error message.
func IsEmptyResult(err error) bool {
	var protonErr *ErrorResponse

	if errors.As(err, &protonErr) {

		if protonErr.Code == 36 {
			return true
		}
	}

	return false
}

func unmarshalGetStreamsEnvelopResp(data []byte) (*GetStreamsEnvelopResp, error) {
	result := new(GetStreamsEnvelopResp)
	if err := json.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("failed to decode list stream response: %w", err)
	}

	for i := range result.Data {
		result.Data[i].TTLExpression = result.Data[i].TTL
	}

	return result, nil
}
