package proton

import (
	"context"
	"time"

	"github.com/reactivex/rxgo/v2"
)

type Proton interface {
	CreateStream(stream StreamDef, streamStorageConfig StreamStorageConfig) error
	CreateExternalStream(stream ExternalStreamDef) error
	UpdateStream(name string, ttl *string, streamStorageConfig *StreamStorageConfig) error
	GetStream(name string) (*GetStreamResp, error)
	GetStreamStats(name string) (*StreamStats, error)
	ListStreams() ([]GetStreamResp, error)
	GetTotalStorage() (int64, error)

	CreateView(view ViewDef) error
	UpdateView(name string, ttl *string, streamStorageConfig *StreamStorageConfig) error
	GetView(name string) (*GetViewResp, error)
	GetViewStats(name string) (*StreamStats, error)
	ListViews() ([]GetViewResp, error)

	// Proton has a consolidated endpoint for stream and view
	DeleteStreamView(name string) error

	IngestRaw(payload []byte, stream string) (int, error)
	IngestEvent(data IngestData, stream string) (int, error)
	IngestEventAsIs(data IngestData, stream string) (int, error)

	Ping() error
	Info() (*InfoResp, error)

	QueryStream(ctx context.Context, startSign <-chan struct{}, sqlStr, id string) ([]Column, rxgo.Observable, rxgo.Observable, error)
	AnalyzeSQL(sql string) (*SQLAnalyzeResult, error)
	GetQueryPipeline(id string) (*GetQueryPipelineResp, error)

	UpsertUDF(udf UDFDef) error
	ListUDF() (*GetUDFsResp, error)
	GetUDF(name string) (*GetUDFResp, error)
	DeleteUDF(name string) error

	StopQuery(id string)

	FormatQuery(sql string) (*FormatQueryResp, error)
}

// proton-go-client
type engine interface {
	Ping() error

	// Run an arbitrary SQL with parameters. For any internal query, please make sure to leverage `sql.NameArg` to
	// prevent SQL injection.
	ExecWithParams(sql string, params ...any) error
	QueryStream(ctx context.Context, startSign <-chan struct{}, sqlStr, id string) ([]Column, rxgo.Observable, rxgo.Observable, error)
	StopQuery(id string)
	SyncQuery(sql string, timeout time.Duration) ([]Column, [][]any, error)
}
