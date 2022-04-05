package materialize

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"

	_ "github.com/lib/pq"

	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const MATERIALIZE_OB_TYPE = "materialize"

type MaterializeObserver struct {
	host       string
	port       int
	user       string
	db         string
	url        string
	query      string
	view       string
	metric     string
	timeFormat string

	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

func NewMaterializeObserver(properties map[string]interface{}) (observer.Observer, error) {
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

	query, err := utils.GetWithDefault(properties, "query", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	view, err := utils.GetWithDefault(properties, "view", "mview")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeFormat, err := utils.GetWithDefault(properties, "time_format", "2006-01-02 15:04:05.000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	url := fmt.Sprintf("postgres://%s@%s:%d/%s", user, host, port, db)

	return &MaterializeObserver{
		host:           host,
		port:           port,
		user:           user,
		db:             db,
		url:            url,
		query:          query,
		view:           view,
		metric:         metric,
		timeFormat:     timeFormat,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(),
	}, nil
}

func (c *MaterializeObserver) getConn() *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), c.url)
	if err != nil {
		log.Logger().Fatal("failed to connect to materialize")
	}
	return conn

}

func (o *MaterializeObserver) observeLatency() error {
	log.Logger().Infof("start observing latency")
	o.metricsManager.Add("latency")
	conn := o.getConn()
	defer conn.Close(context.Background())

	deleteViewSQL := fmt.Sprintf("DROP VIEW %s", o.view)
	if _, err := conn.Exec(context.Background(), deleteViewSQL); err != nil {
		log.Logger().Warnf("drop view failed %w", err)
	}

	condition := fmt.Sprintf("time > '%s'", time.Now().UTC().Format(o.timeFormat))
	createViewSQl := fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s AND %s", o.view, o.query, condition)
	log.Logger().Infof("create view sql is %s", createViewSQl)
	if _, err := conn.Exec(context.Background(), createViewSQl); err != nil {
		log.Logger().Warnf("create view failed %w", err)
		return err
	}

	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Logger().Warnf("create view failed %w", err)
		return err
	}
	defer tx.Rollback(ctx)

	tailQuery := fmt.Sprintf("DECLARE c CURSOR FOR TAIL %s", o.view)
	_, err = tx.Exec(ctx, tailQuery)
	if err != nil {
		log.Logger().Warnf("create tail cur failed %w", err)
		return err
	}

	for {
		if o.isStopped {
			break
		}

		rows, err := tx.Query(ctx, "FETCH ALL c")
		if err != nil {
			log.Logger().Warnf("fetch failed %w", err)
			tx.Rollback(ctx)
			return err
		}

		for rows.Next() {
			var timestamp interface{}
			var diff interface{}
			var value int32
			var eventTime time.Time
			err := rows.Scan(&timestamp, &diff, &value, &eventTime)
			if err != nil {
				log.Logger().Errorf("failed to scan result : %w", err)
				continue
			}
			log.Logger().Infof("%v %v %v %v\n", timestamp, diff, value, eventTime)
			log.Logger().Infof("observe latency %v", time.Until(eventTime))
			o.metricsManager.Observe("latency", -float64(time.Until(eventTime).Microseconds())/1000.0)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Logger().Errorf("failed to commit : %w", err)
	}
	return nil
}

func (o *MaterializeObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")
	conn := o.getConn()
	defer conn.Close(context.Background())

	deleteViewSQL := fmt.Sprintf("DROP VIEW %s", o.view)
	if _, err := conn.Exec(context.Background(), deleteViewSQL); err != nil {
		log.Logger().Warnf("drop view failed %w", err)
	}

	createViewSQl := fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s ", o.view, o.query)
	log.Logger().Infof("create view sql is %s", createViewSQl)
	if _, err := conn.Exec(context.Background(), createViewSQl); err != nil {
		log.Logger().Warnf("create view failed %w", err)
		return err
	}

	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Logger().Warnf("create view failed %w", err)
		return err
	}
	defer tx.Rollback(ctx)

	tailQuery := fmt.Sprintf("DECLARE c CURSOR FOR TAIL %s", o.view)
	_, err = tx.Exec(ctx, tailQuery)
	if err != nil {
		log.Logger().Warnf("create tail cur failed %w", err)
		return err
	}

	for {
		if o.isStopped {
			break
		}

		rows, err := tx.Query(ctx, "FETCH ALL c")
		if err != nil {
			log.Logger().Warnf("fetch failed %w", err)
			tx.Rollback(ctx)
			return err
		}

		for rows.Next() {
			var timestamp interface{}
			var diff1 interface{}
			var diff2 interface{}
			var count int32
			err := rows.Scan(&timestamp, &diff1, &diff2, &count)
			if err != nil {
				log.Logger().Errorf("failed to scan result : %w", err)
				continue
			}

			log.Logger().Infof("count is %d", count)
			o.metricsManager.Observe("throughput", float64(count))
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Logger().Errorf("failed to commit : %w", err)
	}
	return nil
}

func (o *MaterializeObserver) Observe() error {
	log.Logger().Infof("start observing")
	if o.metric == "latency" {
		go o.observeLatency()
	}
	if o.metric == "throughput" {
		go o.observeThroughput()
	}
	return nil
}

func (o *MaterializeObserver) Stop() {
	log.Logger().Infof("call neutron stop observing")
	o.isStopped = true
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("materialize")
}

func (o *MaterializeObserver) Wait() {
	o.obWaiter.Wait()
}
