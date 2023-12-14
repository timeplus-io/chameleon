package kdb

import (
	"fmt"
	"strings"

	"github.com/timeplus-io/chameleon/generator/internal/common"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/source"
	"github.com/timeplus-io/chameleon/generator/internal/utils"

	kdb "github.com/sv/kdbgo"
)

const KDB_SINK_TYPE = "kdb"

type KDBSink struct {
	host string
	port int

	client    *kdb.KDBConn
	columns   []common.Field
	tableName string
}

func NewKDBSink(properties map[string]interface{}) (sink.Sink, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "port", 5001)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	client, err := kdb.DialKDB(host, port, "")
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return &KDBSink{
		host:    host,
		port:    port,
		client:  client,
		columns: nil,
	}, nil
}

func convertType(sourceType string) string {
	switch sourceType {
	case string(source.FIELDTYPE_TIMESTAMP): // TODO: support using datetime instead of symbol for ingest
		return "symbol"
	case string(source.FIELDTYPE_TIMESTAMP_INT):
		return "long"
	case string(source.FIELDTYPE_STRING):
		return "symbol"
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
		return "symbol"
	}
	return "symbol"
}

func (s *KDBSink) Init(name string, fields []common.Field) error {
	log.Logger().Infof("init kdb %s %v", name, fields)
	colums := make([]common.Field, len(fields))
	columsVal := make([]string, len(fields))
	for index, field := range fields {
		colums[index] = common.Field{
			Name: field.Name,
			Type: convertType(field.Type),
		}
		columsVal[index] = fmt.Sprintf("%s:`%s$()", field.Name, colums[index].Type)
	}
	columsVals := strings.Join(columsVal[:], ";")
	creatTableScript := fmt.Sprintf("%s:([]%s)", name, columsVals)

	log.Logger().Infof("init kdb create table %s", creatTableScript)

	res, err := s.client.Call(creatTableScript)
	if err != nil {
		log.Logger().Errorf("query kdb failed: %w", err)
		return err
	}
	s.columns = colums
	s.tableName = name
	log.Logger().Infof("create table result: %v", res)
	return nil
}

func (s *KDBSink) Write(headers []string, rows [][]interface{}, index int) error {
	log.Logger().Debugf("write kdb header:%v rows:%v index:%d", headers, rows, index)
	// order by headers
	values := make(map[string][]string)
	for _, row := range rows {
		for hindex, header := range headers {
			if _, exist := values[header]; !exist {
				values[header] = make([]string, 0)
			}
			values[header] = append(values[header], fmt.Sprintf("%v", row[hindex]))
		}
	}
	log.Logger().Debugf("insert rows values: %v", values)

	results := make([]string, len(headers))
	for index, col := range s.columns {
		if col.Type == "symbol" {
			quotedValue := make([]string, len(values[col.Name]))
			for i, val := range values[col.Name] {
				quotedValue[i] = fmt.Sprintf(`"%s"`, val)
			}
			value := strings.Join(quotedValue, ";")
			log.Logger().Debugf("col %s %s", col.Name, value)
			results[index] = fmt.Sprintf("`$(%s)", value)
		} else {
			value := strings.Join(values[col.Name], " ")
			log.Logger().Debugf("col %s %s", col.Name, value)
			results[index] = value
		}
	}

	insertData := strings.Join(results, ";")
	insertSQL := fmt.Sprintf("`%s insert (%s)", s.tableName, insertData)
	log.Logger().Debugf("insert rows : %s", insertSQL)

	_, err := s.client.Call(insertSQL)
	if err != nil {
		log.Logger().Errorf("insert kdb failed: %w", err)
		return err
	}
	log.Logger().Infof("insert success")
	return nil
}
