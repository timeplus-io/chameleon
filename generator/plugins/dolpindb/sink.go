package dolpindb

import (
	"context"
	"fmt"
	"strings"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"
	"github.com/timeplus-io/chameleon/generator/utils"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
)

const DOLPINDB_SINK_TYPE = "dolpindb"

type DolpinDBSink struct {
	db api.DolphinDB

	dbpath    string
	columns   []common.Field
	tableName string
}

func NewDolpinDBSink(properties map[string]interface{}) (sink.Sink, error) {
	address, err := utils.GetWithDefault(properties, "address", "localhost:8848")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	username, err := utils.GetWithDefault(properties, "username", "admin")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	password, err := utils.GetWithDefault(properties, "password", "123456")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	dbpath, err := utils.GetWithDefault(properties, "dbpath", "dfs://dbtest")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	partitionType, err := utils.GetWithDefault(properties, "partitionType", "RANGE")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	partitionSchema, err := utils.GetWithDefault(properties, "partitionSchema", "0 50 100")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	dbhandle, err := utils.GetWithDefault(properties, "dbhandle", "test")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	engine, err := utils.GetWithDefault(properties, "engine", "OLAP")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	db, err := api.NewDolphinDBClient(context.TODO(), address, nil)
	if err != nil {
		return nil, err
	}

	// connect to server
	err = db.Connect()
	if err != nil {
		return nil, err
	}

	// init login request
	loginReq := &api.LoginRequest{
		UserID:   username,
		Password: password,
	}

	// login dolphindb
	err = db.Login(loginReq)
	if err != nil {
		return nil, err
	}

	existReq := &api.ExistsDatabaseRequest{
		Path: dbpath,
	}

	exist, err := db.ExistsDatabase(existReq)
	if err != nil {
		return nil, err
	}

	if exist {
		dropReq := &api.DropDatabaseRequest{
			Directory: dbpath,
		}

		err := db.DropDatabase(dropReq)
		if err != nil {
			return nil, err
		}
	}

	// init create database request
	dbReq := &api.DatabaseRequest{
		Directory:       dbpath,
		PartitionType:   partitionType,
		PartitionScheme: partitionSchema,
		DBHandle:        dbhandle,
		Engine:          engine,
	}

	// create database
	_, err = db.Database(dbReq)
	if err != nil {
		return nil, err
	}

	return &DolpinDBSink{
		db:      db,
		dbpath:  dbpath,
		columns: nil,
	}, nil
}

func convertType(sourceType string) string {
	switch sourceType {
	case string(source.FIELDTYPE_TIMESTAMP):
		return "TIMESTAMP"
	case string(source.FIELDTYPE_TIMESTAMP_INT):
		return "LONG"
	case string(source.FIELDTYPE_STRING):
		return "STRING"
	case string(source.FIELDTYPE_INT):
		return "INT"
	case string(source.FIELDTYPE_FLOAT):
		return "DOUBLE"
	case string(source.FIELDTYPE_BOOL):
		return "BOOL"
	case string(source.FIELDTYPE_MAP):
	case string(source.FIELDTYPE_ARRAY):
	case string(source.FIELDTYPE_GENERATE):
	case string(source.FIELDTYPE_REGEX):
		return "STRING"
	}
	return "STRING"
}

func (s *DolpinDBSink) Init(name string, fields []common.Field) error {
	log.Logger().Infof("init dolpindb %s %v", name, fields)
	colums := make([]common.Field, len(fields))
	columsVal := make([]string, len(fields))
	for index, field := range fields {
		colums[index] = common.Field{
			Name: field.Name,
			Type: convertType(field.Type),
		}
		columsVal[index] = fmt.Sprintf("%s %s", field.Name, colums[index].Type)
	}
	columsVals := strings.Join(columsVal[:], ",")
	creatTableScript := fmt.Sprintf(`create table "%s"."%s"(%s)`, s.dbpath, name, columsVals)
	log.Logger().Infof("init dolpindb create table %s", creatTableScript)

	res, err := s.db.RunScript(creatTableScript)
	if err != nil {
		log.Logger().Errorf("create table failed: %w", err)
		return err
	}
	s.columns = colums
	s.tableName = name
	log.Logger().Infof("create table result: %v", res)
	return nil
}

func (s *DolpinDBSink) Write(headers []string, rows [][]interface{}, index int) error {
	log.Logger().Infof("write dolpindb header:%v rows:%v %t index:%d", headers, rows, rows, index)

	// order by headers
	values := make(map[string][]interface{})
	for _, row := range rows {
		for hindex, header := range headers {
			if _, exist := values[header]; !exist {
				values[header] = make([]interface{}, 0)
			}
			values[header] = append(values[header], row[hindex])
		}
	}
	log.Logger().Debugf("insert rows values: %v", values)
	colValues := make([]*model.Vector, len(headers))
	colNames := make([]string, len(headers))
	for index, col := range s.columns {
		value := values[col.Name]
		colNames[index] = col.Name

		if col.Type == "LONG" {
			modelType := model.DtLong
			v := make([]int64, len(value))
			for i := 0; i < len(value); i++ {
				v[i] = int64(value[i].(int64))
			}
			datas, err := model.NewDataTypeListWithRaw(modelType, v)
			if err != nil {
				log.Logger().Errorf("create data type failed: %w", err)
				return err
			}
			colValues[index] = model.NewVector(datas)
		} else if col.Type == "INT" {
			modelType := model.DtInt
			v := make([]int32, len(value))
			for i := 0; i < len(value); i++ {
				v[i] = int32(value[i].(int))
			}
			datas, err := model.NewDataTypeListWithRaw(modelType, v)
			if err != nil {
				log.Logger().Errorf("create data type failed: %w", err)
				return err
			}
			colValues[index] = model.NewVector(datas)
		} else if col.Type == "DOUBLE" {
			modelType := model.DtDouble
			v := make([]float64, len(value))
			for i := 0; i < len(value); i++ {
				v[i] = float64(value[i].(float64))
			}
			datas, err := model.NewDataTypeListWithRaw(modelType, v)
			if err != nil {
				log.Logger().Errorf("create data type failed: %w", err)
				return err
			}
			colValues[index] = model.NewVector(datas)
		} else if col.Type == "BOOL" {
			modelType := model.DtBool
			v := make([]bool, len(value))
			for i := 0; i < len(value); i++ {
				v[i] = value[i].(bool)
			}
			datas, err := model.NewDataTypeListWithRaw(modelType, v)
			if err != nil {
				log.Logger().Errorf("create data type failed: %w", err)
				return err
			}
			colValues[index] = model.NewVector(datas)
		} else if col.Type == "TIMESTAMP" {
			modelType := model.DtTimestamp
			v := make([]string, len(value))
			for i := 0; i < len(value); i++ {
				v[i] = value[i].(string)
			}
			datas, err := model.NewDataTypeListWithRaw(modelType, v)
			if err != nil {
				log.Logger().Errorf("create data type failed: %w", err)
				return err
			}
			colValues[index] = model.NewVector(datas)
		} else {
			modelType := model.DtString
			v := make([]string, len(value))
			for i := 0; i < len(value); i++ {
				v[i] = value[i].(string)
			}
			datas, err := model.NewDataTypeListWithRaw(modelType, v)
			if err != nil {
				log.Logger().Errorf("create data type failed: %w", err)
				return err
			}
			colValues[index] = model.NewVector(datas)
		}
	}

	table := model.NewTable(colNames, colValues)
	args := make([]model.DataForm, 1)
	args[0] = table

	res, err := s.db.RunFunc(fmt.Sprintf("tableInsert{loadTable('%s','%s')}", s.dbpath, s.tableName), args)
	if err != nil {
		log.Logger().Errorf("insert dolpin failed: %w", err)
		return err
	}

	log.Logger().Infof("insert success result: %v", res)
	return nil
}
