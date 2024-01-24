package materialize

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timeplus-io/chameleon/generator/internal/common"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/source"
	"github.com/timeplus-io/chameleon/generator/internal/utils"
)

const MATERIALIZE_SINK_TYPE = "materialize"

type MaterializeSink struct {
	host string
	port int
	user string
	db   string
	url  string

	table string
}

func NewMaterializeSink(properties map[string]interface{}) (sink.Sink, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "port", 6875)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	user, err := utils.GetWithDefault(properties, "user", "materialize")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	db, err := utils.GetWithDefault(properties, "db", "materialize")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	url := fmt.Sprintf("postgres://%s@%s:%d/%s", user, host, port, db)

	return &MaterializeSink{
		host: host,
		port: port,
		user: user,
		db:   db,
		url:  url,
	}, nil
}

func (c *MaterializeSink) getConn() *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), c.url)
	if err != nil {
		log.Logger().Fatal("failed to connect to materialize")
	}
	return conn

}

func convertType(sourceType string) string {
	switch sourceType {
	case string(source.FIELDTYPE_TIMESTAMP):
		return "timestamp"
	case string(source.FIELDTYPE_TIMESTAMP_INT):
		return "bigint"
	case string(source.FIELDTYPE_STRING):
		return "text"
	case string(source.FIELDTYPE_INT):
		return "int"
	case string(source.FIELDTYPE_FLOAT):
		return "float"
	case string(source.FIELDTYPE_BOOL):
		return "boolean"
	case string(source.FIELDTYPE_MAP):
	case string(source.FIELDTYPE_ARRAY):
	case string(source.FIELDTYPE_GENERATE):
	case string(source.FIELDTYPE_REGEX):
		return "text"
	}
	return "text"
}

func (s *MaterializeSink) Init(name string, fields []common.Field) error {
	conn := s.getConn()
	defer conn.Close(context.Background())

	s.table = name

	fieldsString := make([]string, len(fields))
	for index, field := range fields {
		fieldsString[index] = fmt.Sprintf("%s %s", field.Name, convertType(field.Type))
	}

	createTableSql := fmt.Sprintf("CREATE TABLE %s (%s)", name, strings.Join(fieldsString, ","))
	log.Logger().Debugf("create table with sql %s", createTableSql)

	// todo: drop table if exist
	if _, err := conn.Exec(context.Background(), createTableSql); err != nil {
		log.Logger().Warnf("create table failed %s", err)
		return err
	}
	return nil
}

func (s *MaterializeSink) Write(headers []string, rows [][]interface{}, index int) error {
	conn := s.getConn() // todo : should share connection here?
	defer conn.Close(context.Background())

	valueStrs := make([]string, len(rows))
	for i, row := range rows {
		rowStrs := make([]string, len(row))
		for j, v := range row {

			switch v.(type) {
			case time.Time:
				rowStrs[j] = fmt.Sprintf("'%v'", v)
			case string:
				rowStrs[j] = fmt.Sprintf("'%s'", v)
			default:
				rowStrs[j] = fmt.Sprint(v)
			}
		}
		valueStrs[i] = fmt.Sprintf("(%s)", strings.Join(rowStrs, ","))
	}

	sql := fmt.Sprintf("insert into %s(%s) values %s",
		s.table, strings.Join(headers, ","), strings.Join(valueStrs, ","))

	log.Logger().Debugf("insert data with sql %s", sql)

	err := conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
		_, err := tx.Exec(context.Background(), sql)
		return err
	})

	if err != nil {
		log.Logger().Error("failed to insert data to materialze", err)
	}
	return nil
}

func (s *MaterializeSink) GetStats() *sink.Stats {
	return &sink.Stats{
		SuccessWrite: 0,
		FailedWrite:  0,
	}
}
