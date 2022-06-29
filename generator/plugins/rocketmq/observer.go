package rocketmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/google/uuid"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const ROCKETMQ_OB_TYPE = "rocketmq"

type RocketMQObserver struct {
	consumer rocketmq.PushConsumer
	topic    string

	metric         string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

func NewRocketMQObserver(properties map[string]interface{}) (observer.Observer, error) {
	brokers, err := utils.GetWithDefault(properties, "brokers", "localhost:9876")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}
	brokerAddress := strings.Split(brokers, ",")

	topic, err := utils.GetWithDefault(properties, "topic", "test")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	groupName := fmt.Sprintf("my-group-%s", uuid.Must(uuid.NewRandom()).String())

	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(groupName),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(brokerAddress)),
	)

	if err != nil {
		return nil, err
	}

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &RocketMQObserver{
		consumer:       c,
		topic:          topic,
		metric:         metric,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(),
	}, nil
}

func (o *RocketMQObserver) observeLatency() error {
	log.Logger().Infof("start observing latency")
	o.metricsManager.Add("latency")
	o.obWaiter.Add(1)
	defer o.obWaiter.Done()

	err := o.consumer.Subscribe(o.topic, consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {
			log.Logger().Infof("subscribe callback: %v", string(msgs[i].Message.Body))
		}

		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return err
	}
	// Note: start after subscribe
	err = o.consumer.Start()
	if err != nil {
		return err
	}

	time.Sleep(time.Hour)
	err = o.consumer.Shutdown()
	if err != nil {
		log.Logger().WithError(err).Warnf("failed to shutdown")
	}
	return nil
}

func (o *RocketMQObserver) Observe() error {
	log.Logger().Infof("start observing")
	if o.metric == "latency" {
		go o.observeLatency()
	}

	return nil
}

func (o *RocketMQObserver) Stop() {
	log.Logger().Infof("call Kafka stop observing")
	o.isStopped = true
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save(ROCKETMQ_OB_TYPE)
}

func (o *RocketMQObserver) Wait() {
	o.obWaiter.Wait()
}
