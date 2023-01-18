package source

import (
	"math/rand"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	fake "github.com/brianvoe/gofakeit/v6"
	rxgo "github.com/reactivex/rxgo/v2"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
)

const MaxUint = ^uint(0)
const MaxInt = int(MaxUint >> 1)

type FieldType string
type TimestampFormatType string

const (
	FIELDTYPE_TIMESTAMP     FieldType = "timestamp"
	FIELDTYPE_TIMESTAMP_INT FieldType = "timestamp_int"
	FIELDTYPE_STRING        FieldType = "string"
	FIELDTYPE_INT           FieldType = "int"
	FIELDTYPE_FLOAT         FieldType = "float"
	FIELDTYPE_BOOL          FieldType = "bool"
	FIELDTYPE_MAP           FieldType = "map"
	FIELDTYPE_ARRAY         FieldType = "array"
	FIELDTYPE_GENERATE      FieldType = "generate"
	FIELDTYPE_REGEX         FieldType = "regex"
)

type Field struct {
	Name              string        `json:"name"`
	Type              FieldType     `json:"type"`
	Range             []interface{} `json:"range,omitempty"`
	Limit             []interface{} `json:"limit,omitempty"`
	TimestampFormat   string        `json:"timestamp_format,omitempty"`
	TimestampDelayMin int           `json:"timestamp_delay_min,omitempty"`
	TimestampDelayMax int           `json:"timestamp_delay_max,omitempty"`
	TimestampLocale   string        `json:"timestamp_locale,omitempty"`
	Rule              string        `json:"rule,omitempty"`
}

type Configuration struct {
	BatchSize     int     `json:"batch_size"`
	Concurrency   int     `json:"concurency"`
	Interval      int     `json:"interval"`
	IntervalDelta int     `json:"interval_delta"`
	BatchNumber   int     `json:"batch_number"`
	Fields        []Field `json:"fields"`
	RandomEvent   bool    `json:"random_event"`
}

type GeneratorEngine struct {
	Config   Configuration
	Finished bool

	streamChannels []chan rxgo.Item
	streams        []rxgo.Observable

	waiter sync.WaitGroup
	lock   sync.Mutex

	cache common.Event
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
	streamChannels := make([]chan rxgo.Item, config.Concurrency)
	streams := make([]rxgo.Observable, config.Concurrency)

	for i := 0; i < config.Concurrency; i++ {
		streamChannel := make(chan rxgo.Item)
		streamChannels[i] = streamChannel
		streams[i] = rxgo.FromChannel(streamChannels[i])
	}

	waiter := sync.WaitGroup{}
	waiter.Add(config.Concurrency)

	if config.BatchNumber == 0 {
		config.BatchNumber = MaxInt
	}

	return &GeneratorEngine{
		Config:         config,
		Finished:       false,
		streamChannels: streamChannels,
		streams:        streams,
		waiter:         waiter,
		lock:           sync.Mutex{},
		cache:          nil,
	}, nil
}

func DefaultConfiguration() Configuration {
	defaultConfiguration := Configuration{
		BatchSize:   3,
		BatchNumber: 100,
		Concurrency: 1,
		Interval:    1000,
		Fields: []Field{
			{
				Name:  "number",
				Type:  FIELDTYPE_INT,
				Limit: []interface{}{float64(0), float64(10)},
			},
			{
				Name:            "time",
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
		go func(index int) {
			s.run(index)
		}(i)
	}
}

func (s *GeneratorEngine) run(index int) error {
	log.Logger().Infof("start generate routine with index %d, batch number %d ", index, s.Config.BatchNumber)
	streamChannel := s.streamChannels[index]
	for i := 0; i < s.Config.BatchNumber; i++ {
		if s.Finished {
			log.Logger().Warnf("run generator finished %d", index)
			break
		}
		events := s.generateBatchEvent()
		streamChannel <- rxgo.Of(events)
		if s.Config.IntervalDelta > 0 {
			interval := faker.IntRange(s.Config.Interval-s.Config.IntervalDelta, s.Config.Interval+s.Config.IntervalDelta)
			time.Sleep(time.Duration(interval) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(s.Config.Interval) * time.Millisecond)
		}
	}
	close(streamChannel)
	return nil
}

func (s *GeneratorEngine) Stop() {
	s.Finished = true
}

func (s *GeneratorEngine) GetStreams() []rxgo.Observable {
	return s.streams
}

func (s *GeneratorEngine) Read() []common.Event {
	result := make([]common.Event, 0)
	for i := 0; i < s.Config.Concurrency; i++ {
		observable := s.streams[i].Take(1) // must after generate start
		for item := range observable.Observe() {
			result = append(result, item.V.(common.Event))
		}
	}

	return result
}

func (s *GeneratorEngine) IsFinished() bool {
	return s.Finished
}

func (s *GeneratorEngine) GetFields() []common.Field {
	fields := make([]common.Field, len(s.Config.Fields))

	for index, field := range s.Config.Fields {
		fields[index] = common.Field{
			Name: field.Name,
			Type: string(field.Type),
		}
	}

	return fields
}

func makeTimestampInt(timestampDeleyMin int, timestampDeleyMax int) int64 {
	now := time.Now().UTC()
	// delay := faker.Number(timestampDeleyMin, timestampDeleyMax)
	nsec := now.UnixMilli()
	//t := nsec - int64(delay)
	return nsec
}

func makeTimestamp(timestampDeleyMin int, timestampDeleyMax int) time.Time {
	t := makeTimestampInt(timestampDeleyMin, timestampDeleyMax)
	tm := time.UnixMilli(int64(t)).UTC()
	return tm
}

func makeTimestampString(format string, timestampDeleyMin int, timestampDeleyMax int, locale string) string {
	t := makeTimestampInt(timestampDeleyMin, timestampDeleyMax)

	// location: "America/Los_Angeles"
	if locale != "" {
		location, err := time.LoadLocation(locale)
		if err != nil {
			panic(err)
		}

		timestamp := time.UnixMilli(int64(t)).In(location)
		return timestamp.Format(format)
	}

	timestamp := time.UnixMilli(int64(t)).UTC()
	result := timestamp.Format(format)
	return result
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
	result["key5"] = makeTimestampString("2006-01-02 15:04:05.000", 0, 0, "")

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
	timestampFormat string, timestampDelayMin int, timestampDelayMax int, timestampLocale string, rule string) interface{} {
	switch s := sourceType; s {
	case FIELDTYPE_TIMESTAMP:
		if timestampFormat == "" {
			return makeTimestamp(timestampDelayMin, timestampDelayMax)
		} else {
			return makeTimestampString(timestampFormat, timestampDelayMin, timestampDelayMax, timestampLocale)
		}

	case FIELDTYPE_TIMESTAMP_INT:
		return makeTimestampInt(timestampDelayMin, timestampDelayMax)

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
	// cache event expect time fields
	if !s.Config.RandomEvent && s.cache != nil {
		event := make(common.Event)
		for k, v := range s.cache {
			event[k] = v
		}

		// keep time and value random as these are critical for latency caculation
		for _, f := range s.Config.Fields {
			if f.Name == "time" || f.Name == "value" {
				event[f.Name] = makeValue(f.Type, f.Range, f.Limit, f.TimestampFormat, f.TimestampDelayMin, f.TimestampDelayMax, f.TimestampLocale, f.Rule)
			}
		}
		return event
	}

	value := make(common.Event)
	fields := s.Config.Fields

	for _, f := range fields {
		value[f.Name] = makeValue(f.Type, f.Range, f.Limit, f.TimestampFormat, f.TimestampDelayMin, f.TimestampDelayMax, f.TimestampLocale, f.Rule)
	}

	s.cache = value
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
