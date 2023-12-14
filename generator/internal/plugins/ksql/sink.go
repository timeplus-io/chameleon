package ksql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rmoff/ksqldb-go"

	"github.com/timeplus-io/chameleon/generator/internal/common"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/plugins/kafka"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/source"
	"github.com/timeplus-io/chameleon/generator/internal/utils"
)

const KSQL_SINK_TYPE = "ksql"

type KSQLSink struct {
	host        string
	port        int
	usingBroker bool
	brokers     string
	stream      string
	client      *ksqldb.Client
	ctx         context.Context
	brokerSink  sink.Sink
}

func NewKSQLSink(properties map[string]interface{}) (sink.Sink, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "port", 8088)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	useBorker, err := utils.GetBoolWithDefault(properties, "use_broker", false)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	brokers, err := utils.GetWithDefault(properties, "brokers", "localhost:9092")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	url := fmt.Sprintf("http://%s:%d", host, port)
	client := ksqldb.NewClient(url, "", "")

	return &KSQLSink{
		host:        host,
		port:        port,
		usingBroker: useBorker,
		brokers:     brokers,
		client:      client,
		ctx:         context.Background(),
	}, nil
}

func convertType(sourceType string) string {
	switch sourceType {
	case string(source.FIELDTYPE_TIMESTAMP):
		return "STRING"
	case string(source.FIELDTYPE_TIMESTAMP_INT):
		return "bigint"
	case string(source.FIELDTYPE_STRING):
		return "STRING"
	case string(source.FIELDTYPE_INT):
		return "int"
	case string(source.FIELDTYPE_FLOAT):
		return "DOUBLE"
	case string(source.FIELDTYPE_BOOL):
		return "BOOLEAN"
	case string(source.FIELDTYPE_MAP):
	case string(source.FIELDTYPE_ARRAY):
	case string(source.FIELDTYPE_GENERATE):
	case string(source.FIELDTYPE_REGEX):
		return "STRING"
	}
	return "STRING"
}

func (s *KSQLSink) Init(name string, fields []common.Field) error {
	if s.usingBroker {
		properties := map[string]interface{}{
			"brokers": s.brokers,
		}
		if brokerSink, err := kafka.NewKafkaSink(properties); err != nil {
			log.Logger().Error("failed to create broker sink : %w", err)
		} else {
			s.brokerSink = brokerSink
			s.brokerSink.Init(name, fields)
		}
	}

	// waiting topic deletion
	time.Sleep(5 * time.Second)

	s.stream = name
	fieldsString := make([]string, len(fields))
	for index, field := range fields {
		fieldsString[index] = fmt.Sprintf("%s %s", field.Name, convertType(field.Type))
	}

	dropStreamSql := fmt.Sprintf("DROP STREAM %s;", name)
	if err := s.client.Execute(dropStreamSql); err != nil {
		log.Logger().Warnf("drop stream failed : %w", err)
	}

	createStreamSql := fmt.Sprintf("CREATE STREAM %s (%s) WITH (KAFKA_TOPIC='%s', PARTITIONS=1, VALUE_FORMAT='JSON');", name, strings.Join(fieldsString, ","), name)
	log.Logger().Debugf("create stream with sql %s", createStreamSql)

	if err := s.client.Execute(createStreamSql); err != nil {
		log.Logger().Errorf("create stream failed : %w", err)
		return err
	}

	log.Logger().Infof("create stream %s success", name)

	return nil
}

func (s *KSQLSink) Write(headers []string, rows [][]interface{}, index int) error {
	if s.usingBroker {
		return s.writeBroker(headers, rows, index)
	}

	return s.writeSQL(headers, rows, index)
}

func (s *KSQLSink) writeSQL(headers []string, rows [][]interface{}, index int) error {
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
		sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s;",
			s.stream, strings.Join(headers, ","), valueStrs[i])

		log.Logger().Debugf("insert data with sql %s", sql)

		if err := s.client.Execute(sql); err != nil {
			log.Logger().Errorf("failed to insert : %w", err)
			return err
		}
	}

	return nil
}

func (s *KSQLSink) writeBroker(headers []string, rows [][]interface{}, index int) error {
	return s.brokerSink.Write(headers, rows, index)
}
