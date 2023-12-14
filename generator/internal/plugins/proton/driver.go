package proton

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/spf13/viper"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	protonDriver "github.com/timeplus-io/proton-go-driver/v2"
)

var (
	codeRe = *regexp.MustCompile(`code: (.+[0-9])`)
	msgRe  = *regexp.MustCompile(`message: (.*)`)
)

type Column struct {
	Name string `json:"name" binding:"required"`
	Type string `json:"type" binding:"required"`
} // @name Column

// For Proton query response
type ResponseDataRow []any

type config struct {
	Host            string `json:"host,omitempty"`
	Port            int    `json:"port,omitempty"`
	User            string `json:"user,omitempty"`
	Password        string `json:"password,omitempty"`
	Debug           bool   `json:"debug,omitempty"`
	FloatFormat     string `json:"float_format,omitempty"`
	TimeFormat      string `json:"time_format,omitempty"`
	MaxIdleConns    int    `json:"max_idle_conns,omitempty"`
	ConnMaxLifetime int    `json:"conn_max_lifetime,omitempty"`
	ConnMaxIdleTime int    `json:"conn_max_idle_time,omitempty"`
}

func NewConfig(host, user, password string) config {
	return config{
		Host:            host,
		Port:            viper.GetInt("proton-tcp-port"),
		User:            user,
		Password:        password,
		MaxIdleConns:    viper.GetInt("proton-max-idle-conns"),
		ConnMaxLifetime: viper.GetInt("proton-conn-max-lifetime"),
		ConnMaxIdleTime: viper.GetInt("proton-conn-max-idle-time"),
	}
}

type Engine struct {
	connection *sql.DB
}

func NewEngine(c config) *Engine {
	if c.FloatFormat == "" {
		c.FloatFormat = "%v"
	}

	connection := protonDriver.OpenDB(&protonDriver.Options{
		Addr: []string{fmt.Sprintf("%s:%d", c.Host, c.Port)},
		Auth: protonDriver.Auth{
			Username: c.User,
			Password: c.Password,
		},
		DialTimeout: 5 * time.Second,
		Debug:       c.Debug,
	})

	connection.SetMaxIdleConns(c.MaxIdleConns)
	connection.SetConnMaxLifetime(time.Duration(c.ConnMaxLifetime) * time.Second)
	connection.SetConnMaxIdleTime(time.Duration(c.ConnMaxIdleTime) * time.Second)

	db := Engine{
		connection: connection,
	}

	return &db
}

func (e *Engine) Ping() error {
	return e.connection.Ping()
}

func (e *Engine) ExecWithParams(sql string, params ...any) error {
	log.Logger().Debugf("run exec %s", sql)
	if _, err := e.connection.Exec(sql, params...); err != nil {
		return e.handleDriverError(err)
	}
	return nil
}

func Parse(err error) (int, string) {
	var code int
	var msg string

	errStr := err.Error()
	codeMatches := codeRe.FindStringSubmatch(errStr)
	if len(codeMatches) == 2 {
		code, _ = strconv.Atoi(codeMatches[1])
	}

	msgMatches := msgRe.FindStringSubmatch(errStr)
	if len(msgMatches) == 2 {
		msg = msgMatches[1]
	}

	return code, msg
}

func IsContextCancel(err error) bool {
	code, msg := Parse(err)
	return code == 394 && strings.Contains(msg, "Query was cancelled")
}

func (e *Engine) QueryStream(ctx context.Context, sqlStr, id string) ([]Column, rxgo.Observable, rxgo.Observable, error) {
	logger := log.Logger().WithField("ID", id)
	logger.Debug("run streaming query")

	progressCh := make(chan rxgo.Item)
	ckCtx := protonDriver.Context(ctx, protonDriver.WithQueryID(id), protonDriver.WithProgress(func(p *protonDriver.Progress) {
		progressCh <- rxgo.Of(p)
	}))

	rows, err := e.connection.QueryContext(ckCtx, sqlStr)
	if err != nil {
		return []Column{}, nil, nil, e.handleDriverError(err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		e.StopQuery(id)
		return []Column{}, nil, nil, fmt.Errorf("failed to get column type: %w", err)
	}

	count := len(columnTypes)
	header := make([]Column, count)

	values := make(ResponseDataRow, count) // values is raw data
	valuePtrs := make([]any, count)

	for i, col := range columnTypes {
		header[i] = Column{
			Name: col.Name(),
			Type: col.DatabaseTypeName(),
		}

		valuePtrs[i] = &values[i]
	}

	dataCh := make(chan rxgo.Item)

	dataObservable := rxgo.FromChannel(dataCh, rxgo.WithPublishStrategy())
	progressObservable := rxgo.FromEventSource(progressCh)

	go func() {
		defer func() {
			close(dataCh)
			close(progressCh)
		}()

		// Be extra cautious on the following for loop. Anything inside the for loop will be executed for every single row.
		for rows.Next() {
			if err = rows.Scan(valuePtrs...); err != nil {
				dataCh <- rxgo.Error(err)
				continue
			}

			// Copy the `values` to a new slice and emit it so that the `values` can keep receiving new row
			row := make(ResponseDataRow, count)
			copy(row, values)

			dataCh <- rxgo.Of(row)
		}

		if err := rows.Close(); err != nil {
			dataCh <- rxgo.Error(err)
			return
		}

		if err := rows.Err(); err != nil {
			if errors.Is(err, context.Canceled) || IsContextCancel(err) {
				dataCh <- rxgo.Error(context.Canceled)
			} else {
				dataCh <- rxgo.Error(err)
			}
			return
		}

	}()

	return header, dataObservable, progressObservable, nil
}

func (e *Engine) StopQuery(id string) {
	if err := e.ExecWithParams("kill query where query_id=@id", sql.Named("id", id)); err != nil {
		log.Logger().Errorf("failed to kill query %s: %s", id, err)
	}
}

// Be sure ONLY use this function when the result set is small AND the query is fast.
// To make it simple, we allow the caller to pass a timeout. Ideally it should accept a context and let the caller to cancel it
func (e *Engine) SyncQuery(sql string, timeout time.Duration, params ...any) ([]Column, [][]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, err := e.connection.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, nil, e.handleDriverError(err)
	}

	header, err := e.readHeader(rows)
	if err != nil {
		return nil, nil, err
	}

	colCount := len(header)
	values := make(ResponseDataRow, colCount) // values is raw data
	valuePtrs := make([]any, colCount)
	for i := range header {
		valuePtrs[i] = &values[i]
	}

	data := make([][]any, 0)
	for rows.Next() {
		if err = rows.Scan(valuePtrs...); err != nil {
			return []Column{}, nil, err
		}

		row := make(ResponseDataRow, colCount)
		copy(row, values)

		data = append(data, row)
	}

	if err := rows.Close(); err != nil {
		return nil, nil, err
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return header, data, nil
}

func (e *Engine) handleDriverError(err error) error {
	ex := new(protonDriver.Exception)
	if errors.As(err, &ex) {
		protonErr := ErrorResponse{
			HTTPStatusCode: -1,
			Code:           int(ex.Code),
			Message:        ex.Message,
		}

		return &protonErr
	}

	return err
}

func (e *Engine) readHeader(rows *sql.Rows) ([]Column, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	header := make([]Column, len(columnTypes))

	for i, col := range columnTypes {
		header[i] = Column{
			Name: col.Name(),
			Type: col.DatabaseTypeName(),
		}
	}

	return header, nil
}
