package source

import (
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	fake "github.com/brianvoe/gofakeit/v6"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
)

type FieldType string
type TimestampFormatType string

const (
	FIELDTYPE_TIMESTAMP FieldType = "timestamp"
	FIELDTYPE_STRING    FieldType = "string"
	FIELDTYPE_INT       FieldType = "int"
	FIELDTYPE_FLOAT     FieldType = "float"
	FIELDTYPE_BOOL      FieldType = "bool"
	FIELDTYPE_MAP       FieldType = "map"
	FIELDTYPE_ARRAY     FieldType = "array"
	FIELDTYPE_GENERATE  FieldType = "generate"
	FIELDTYPE_REGEX     FieldType = "regex"
)

type Field struct {
	Name              string        `json:"name"`
	Type              FieldType     `json:"type"`
	Range             []interface{} `json:"range,omitempty"`
	Limit             []interface{} `json:"limit,omitempty"`
	TimestampFormat   string        `json:"timestamp_format,omitempty"`
	TimestampDelayMin int           `json:"timestamp_delay_min,omitempty"`
	TimestampDelayMax int           `json:"timestamp_delay_max,omitempty"`
	Rule              string        `json:"rule,omitempty"`
}

type Configuration struct {
	BatchSize     int     `json:"batch_size"`
	Concurrency   int     `json:"concurency"`
	Interval      int     `json:"interval"`
	IntervalDelta int     `json:"interval_delta"`
	Count         int     `json:"count"`
	Fields        []Field `json:"fields"`
}

type GeneratorEngine struct {
	Config   Configuration
	Finished bool

	streamChannels []chan []common.Event
}

var faker *fake.Faker

func init() {
	fake.AddFuncLookup("byear", fake.Info{
		Category:    "custom birthday year",
		Description: "year between 1925 to 2002",
		Example:     "1950",
		Output:      "int",
		Generate: func(r *rand.Rand, m *fake.MapParams, info *fake.Info) (interface{}, error) {
			return gofakeit.IntRange(1925, 2002), nil
		},
	})
	faker = fake.New(0)
}

func NewGenarator(config Configuration) (*GeneratorEngine, error) {

	streamChannels := make([]chan []common.Event, config.Concurrency)
	return &GeneratorEngine{
		Config:         config,
		Finished:       false,
		streamChannels: streamChannels,
	}, nil
}

func DefaultConfiguration() Configuration {
	defaultConfiguration := Configuration{
		BatchSize:   3,
		Concurrency: 1,
		Interval:    1000,
		Fields: []Field{
			{
				Name:  "number",
				Type:  FIELDTYPE_INT,
				Limit: []interface{}{float64(0), float64(10)},
			},
			{
				Name:            "_time",
				Type:            FIELDTYPE_TIMESTAMP,
				TimestampFormat: "2006-01-02 15:04:05.000",
			},
		},
	}
	return defaultConfiguration
}

func (s *GeneratorEngine) Start() {
	s.Finished = false // lock?
	for i := 0; i < s.Config.Concurrency; i++ {
		go s.run(i)
	}
}

func (s *GeneratorEngine) run(index int) error {
	log.Logger().Infof("start generate routine with index %d", index)
	streamChannel := make(chan []common.Event)
	s.streamChannels[index] = streamChannel
	go func() {
		count := 0
		for {
			if s.Finished {
				break
			}
			events := s.generateBatchEvent()
			streamChannel <- events
			count += len(events)

			if s.Config.IntervalDelta > 0 {
				interval := faker.IntRange(s.Config.Interval-s.Config.IntervalDelta, s.Config.Interval+s.Config.IntervalDelta)
				time.Sleep(time.Duration(interval) * time.Millisecond)
			} else {
				time.Sleep(time.Duration(s.Config.Interval) * time.Millisecond)
			}

			if s.Config.Count != 0 {
				if count >= s.Config.Count {
					s.Finished = true // notify source generation completed
					break
				}
			}
		}
		close(streamChannel)
	}()
	return nil
}

func (s *GeneratorEngine) Stop() {
	s.Finished = true
}

func (s *GeneratorEngine) Read() []common.Event {
	result := make([]common.Event, 0)
	for i := 0; i < s.Config.Concurrency; i++ {
		events := <-s.streamChannels[i]
		result = append(result, events...)
	}

	return result
}

func (s *GeneratorEngine) IsFinished() bool {
	return s.Finished
}

func makeTimestampInt(timestampDeleyMin int, timestampDeleyMax int) int64 {
	now := time.Now().UTC()
	delay := faker.Number(timestampDeleyMin, timestampDeleyMax)
	nsec := now.UnixMilli()
	t := nsec - int64(delay)
	return t
}

func makeTimestamp(timestampDeleyMin int, timestampDeleyMax int) time.Time {
	t := makeTimestampInt(timestampDeleyMin, timestampDeleyMax)
	tm := time.UnixMilli(int64(t)).UTC()
	return tm
}

func makeTimestampString(format string, timestampDeleyMin int, timestampDeleyMax int) string {
	t := makeTimestampInt(timestampDeleyMin, timestampDeleyMax)
	timestamp := time.UnixMilli(int64(t)).UTC()

	return timestamp.Format(format)
}

func makeInt(ranges []int, limits []int) int {
	range_length := len(ranges)
	limit_length := len(limits)

	if range_length > 0 {
		index := faker.Number(0, range_length-1)
		return (ranges)[index]
	} else if limit_length > 1 {
		return faker.Number((limits)[0], (limits)[1])
	}

	return 0
}

func makeFloat(ranges []float32, limits []float32) float32 {
	range_length := len(ranges)
	limit_length := len(limits)

	if range_length > 0 {
		index := faker.Number(0, range_length-1)
		return ranges[index]
	} else if limit_length > 1 {
		return faker.Float32Range(limits[0], limits[1])
	}

	return 0.0
}

func makeBool() bool {
	return faker.Bool()
}

func makeString(ranges []string) string {
	range_length := len(ranges)

	if range_length > 0 {
		return faker.RandomString(ranges)
	}

	return faker.LetterN(8)
}

func makeMap() map[string]interface{} {
	result := make(map[string]interface{})
	result["key1"] = makeBool()
	result["key2"] = makeInt([]int{}, []int{0, 10})
	result["key3"] = makeString([]string{})
	result["key4"] = makeTimestamp(0, 0)
	result["key5"] = makeTimestampString("2006-01-02 15:04:05.000", 0, 0)

	return result
}

func makeArray() []interface{} {
	result := make([]interface{}, 3)
	for i := 0; i < 3; i++ {
		result[i] = makeInt([]int{}, []int{0, 10})
	}

	return result
}

func makeGenerate(rule string) string {
	return faker.Generate(rule)
}

func makeRegex(rule string) string {
	return faker.Regex(rule)
}

func makeValue(sourceType FieldType, sourceRange []interface{}, sourceLimit []interface{},
	timestampFormat string, timestampDelayMin int, timestampDelayMax int, rule string) interface{} {
	switch s := sourceType; s {
	case FIELDTYPE_TIMESTAMP:
		if timestampFormat == "" {
			return makeTimestamp(timestampDelayMin, timestampDelayMax)
		} else if timestampFormat == "int" {
			return makeTimestampInt(timestampDelayMin, timestampDelayMax)
		} else {
			return makeTimestampString(timestampFormat, timestampDelayMin, timestampDelayMax)
		}

	case FIELDTYPE_STRING:
		ranges := make([]string, len(sourceRange))
		for i := 0; i < len(sourceRange); i++ {
			ranges[i] = sourceRange[i].(string)
		}

		return makeString(ranges)
	case FIELDTYPE_INT:
		ranges := make([]int, len(sourceRange))
		for i := 0; i < len(sourceRange); i++ {
			ranges[i] = int(sourceRange[i].(float64))
		}

		limits := make([]int, len(sourceLimit))
		for i := 0; i < len(sourceLimit); i++ {
			limits[i] = int(sourceLimit[i].(float64))
		}
		return makeInt(ranges, limits)
	case FIELDTYPE_FLOAT:
		ranges := make([]float32, len(sourceRange))
		for i := 0; i < len(sourceRange); i++ {
			ranges[i] = float32(sourceRange[i].(float64))
		}

		limits := make([]float32, len(sourceLimit))
		for i := 0; i < len(sourceLimit); i++ {
			limits[i] = float32(sourceLimit[i].(float64))
		}
		return makeFloat(ranges, limits)
	case FIELDTYPE_BOOL:
		return makeBool()
	case FIELDTYPE_MAP:
		return makeMap()
	case FIELDTYPE_ARRAY:
		return makeArray()
	case FIELDTYPE_GENERATE:
		return makeGenerate(rule)
	case FIELDTYPE_REGEX:
		return makeRegex(rule)
	default:
		return nil
	}
}

func (s *GeneratorEngine) generateEvent() common.Event {
	value := make(common.Event)
	fields := s.Config.Fields

	for _, f := range fields {
		value[f.Name] = makeValue(f.Type, f.Range, f.Limit, f.TimestampFormat, f.TimestampDelayMin, f.TimestampDelayMax, f.Rule)
	}
	return value
}

func (s *GeneratorEngine) generateBatchEvent() []common.Event {
	batchSize := s.Config.BatchSize
	events := make([]common.Event, batchSize)

	for i := 0; i < batchSize; i++ {
		events[i] = s.generateEvent()
	}
	return events
}
