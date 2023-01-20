package dolpindb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/google/uuid"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const DOLPINEB_OB_TYPE = "dolpindb"

type DolpinDBObserver struct {
	dbpath    string
	tableName string

	db api.DolphinDB

	metric         string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager metrics.Metrics
}

func NewKDolpinDBObserver(properties map[string]interface{}) (observer.Observer, error) {
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

	tableName, err := utils.GetWithDefault(properties, "table", "test")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	dbhandle, err := utils.GetWithDefault(properties, "dbhandle", "test")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
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

	// init create database request
	dbReq := &api.DatabaseRequest{
		Directory: dbpath,
		DBHandle:  dbhandle,
	}

	// get database
	_, err = db.Database(dbReq)
	if err != nil {
		return nil, err
	}

	var metricsManager metrics.Metrics
	if _, ok := properties["metric_store_address"]; !ok {
		metricsManager = metrics.NewCSVMetricManager()
	} else {
		metricStoreAddress, err := utils.GetWithDefault(properties, "metric_store_address", "http://localhost:8000")
		if err != nil {
			return nil, fmt.Errorf("invalid properties : %w", err)
		}

		metricStoreAPIKey, err := utils.GetWithDefault(properties, "metric_store_apikey", "")
		if err != nil {
			return nil, fmt.Errorf("invalid properties : %w", err)
		}

		metricStoreTenant, err := utils.GetWithDefault(properties, "metric_store_tenant", "")
		if err != nil {
			return nil, fmt.Errorf("invalid properties : %w", err)
		}

		metricsManager = metrics.NewTimeplusMetricManager(metricStoreAddress, metricStoreTenant, metricStoreAPIKey)
	}

	return &DolpinDBObserver{
		db:             db,
		dbpath:         dbpath,
		tableName:      tableName,
		metric:         metric,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metricsManager,
	}, nil
}

func (o *DolpinDBObserver) observeLatency() error {
	log.Logger().Errorf("latency observing not supported")
	return fmt.Errorf("latency  not supported")
}

func (o *DolpinDBObserver) observeThroughput() error {
	log.Logger().Errorf("throughput observing not supported")
	return fmt.Errorf("throughput not supported")
}

func (o *DolpinDBObserver) observeAvailability() error {
	log.Logger().Infof("availability observing started")
	o.metricsManager.Add("availability")

	o.obWaiter.Add(1)
	defer o.obWaiter.Done()

	metricsName := "availability"
	id, _ := uuid.NewRandom()
	tag := map[string]interface{}{"targte": "dolphinDB", "testId": id.String()}

	var preCount int32 = 0
	for {
		tb, err := o.db.RunScript(fmt.Sprintf("select count(*) from loadTable('%s','%s')", o.dbpath, o.tableName))
		if err != nil {
			log.Logger().Errorf("failed to run script %s", err)
		} else {
			tbr := tb.(*model.Table)
			count := tbr.GetColumnByIndex(0).Get(0).Value().(int32)
			log.Logger().Infof("ob result count is %v", count)
			o.metricsManager.Observe(metricsName, float64(count), tag)

			if preCount != 0 && count == int32(preCount) {
				break
			} else {
				preCount = count
			}
		}

		time.Sleep(2 * time.Second)
	}

	time.Sleep(100 * time.Millisecond)
	o.metricsManager.Flush()

	return nil
}

func (o *DolpinDBObserver) Observe() error {
	log.Logger().Infof("start observing")
	if o.metric == "latency" {
		go o.observeLatency()
	}

	if o.metric == "throughput" {
		go o.observeThroughput()
	}

	if o.metric == "availability" {
		go o.observeAvailability()
	}

	return nil
}

func (o *DolpinDBObserver) Stop() {
	log.Logger().Infof("call dolphinDB stop observing")
	o.isStopped = true
	log.Logger().Infof("set stopped")
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("dolphinDB")
	log.Logger().Infof("save observing completed")
}

func (o *DolpinDBObserver) Wait() {
	o.obWaiter.Wait()
}
