package rocketmq

import (
	"context"
	"fmt"
	"strings"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const ROCKETMQ_SINK_TYPE = "rocketmq"

type RocketMQSink struct {
	producer rocketmq.Producer
	topic    string
}

func NewRocketMQSink(properties map[string]interface{}) (sink.Sink, error) {
	brokers, err := utils.GetWithDefault(properties, "brokers", "localhost:9876")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	brokerAddress := strings.Split(brokers, ",")

	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver(brokerAddress)),
		producer.WithRetry(2),
	)

	if err != nil {
		return nil, err
	}

	return &RocketMQSink{producer: p}, nil
}

func (s *RocketMQSink) Init(name string, fields []common.Field) error {
	//TODO: there is no shut down for sink interface for now
	s.topic = name
	return s.producer.Start()
}

func (s *RocketMQSink) Write(headers []string, rows [][]interface{}, index int) error {
	events := common.ToEvents(headers, rows)
	errs := make([]error, 0)
	for _, event := range events {
		msg := &primitive.Message{
			Topic: s.topic,
			Body:  []byte(event.String()),
		}

		_, err := s.producer.SendSync(context.Background(), msg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		log.Logger().WithError(errs[0]).Error("failed to send message to rocketmq broker")
		return errs[0]
	}
	return nil
}
