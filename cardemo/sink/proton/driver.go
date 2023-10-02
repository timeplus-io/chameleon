package proton

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/reactivex/rxgo/v2"
	protonDriver "github.com/timeplus-io/proton-go-driver/v2"

	"github.com/timeplus-io/chameleon/cardemo/log"
)

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

func (e *Engine) QueryStream(ctx context.Context, startSign <-chan struct{}, sqlStr, id string) ([]Column, rxgo.Observable, rxgo.Observable, error) {
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

		<-startSign

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

// A handy wrapper of `QueryStream` function to return all the results all together.
// Be sure ONLY use this function when the result set is small AND the query is fast.
// To make it simple, we allow the caller to pass a timeout. Ideally it should accept a context and let the caller to cancel it
func (e *Engine) SyncQuery(sql string, timeout time.Duration) ([]Column, [][]any, error) {
	id := uuid.Must(uuid.NewRandom()).String()

	startSign := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cols, stream, _, err := e.QueryStream(ctx, startSign, sql, id)
	if err != nil {
		cancel()
		return nil, nil, e.handleDriverError(err)
	}

	var events [][]any
	var queryErr error

	stream.ForEach(func(v any) {
		event := v.(ResponseDataRow)
		events = append(events, event)
	}, func(err error) {
		queryErr = err
	}, func() {
		cancel()
		e.StopQuery(id)
	})

	go func() {
		time.Sleep(timeout)
		cancel()
		e.StopQuery(id)
	}()

	stream.Connect(ctx)
	close(startSign)
	<-ctx.Done()

	if queryErr != nil {
		return nil, nil, queryErr
	}

	return cols, events, nil
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
