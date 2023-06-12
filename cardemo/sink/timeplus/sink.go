package timeplus

import (
	"fmt"
	"time"

	"github.com/timeplus-io/chameleon/cardemo/common"
	"github.com/timeplus-io/chameleon/cardemo/log"

	"github.com/timeplus-io/chameleon/cardemo/utils"

	"github.com/timeplus-io/go-client/timeplus"
)

const DefaultTTL = "to_datetime(_tp_time) + INTERVAL 30 DAY"
const DefaultLogStoreRetentionBytes = 604800000
const DefaultLogStoreRetentionMS = 1342177280

var DimCarStreamDef = timeplus.StreamDef{
	Name: "dim_car_info",
	Columns: []timeplus.ColumnDef{
		{
			Name: "in_service",
			Type: "bool",
		},
		{
			Name: "cid",
			Type: "string",
		},
		{
			Name: "license_plate_no",
			Type: "string",
		},
	},
	TTLExpression:          DefaultTTL,
	LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
	LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
}

var DimUserStreamDef = timeplus.StreamDef{
	Name: "dim_user_info",
	Columns: []timeplus.ColumnDef{
		{
			Name: "birthday",
			Type: "string",
		},
		{
			Name: "uid",
			Type: "string",
		},
		{
			Name: "first_name",
			Type: "string",
		},
		{
			Name: "last_name",
			Type: "string",
		},
		{
			Name: "email",
			Type: "string",
		},
		{
			Name: "credit_card",
			Type: "string",
		},
		{
			Name: "gender",
			Type: "string",
		},
	},
	TTLExpression:          DefaultTTL,
	LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
	LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
}

var BookingStreamDef = timeplus.StreamDef{
	Name: "bookings",
	Columns: []timeplus.ColumnDef{
		{
			Name: "action",
			Type: "string",
		},
		{
			Name: "expire",
			Type: "datetime64(3)",
		},
		{
			Name: "bid",
			Type: "string",
		},
		{
			Name: "time",
			Type: "datetime64(3)",
		},
		{
			Name: "email",
			Type: "string",
		},
		{
			Name: "uid",
			Type: "string",
		},
		{
			Name: "cid",
			Type: "string",
		},
		{
			Name: "booking_time",
			Type: "datetime64(3)",
		},
		{
			Name:    "_tp_time",
			Type:    "datetime64(3)",
			Default: "time",
		},
	},
	TTLExpression:          DefaultTTL,
	LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
	LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
}

var CarStream = timeplus.StreamDef{
	Name: "car_live_data",
	Columns: []timeplus.ColumnDef{
		{
			Name: "in_use",
			Type: "bool",
		},
		{
			Name: "longitude",
			Type: "float",
		},
		{
			Name: "latitude",
			Type: "float",
		},
		{
			Name: "speed_kmh",
			Type: "uint32",
		},
		{
			Name: "gas_percent",
			Type: "decimal(10, 2)",
		},
		{
			Name: "total_km",
			Type: "float",
		},
		{
			Name: "locked",
			Type: "bool",
		},
		{
			Name: "cid",
			Type: "string",
		},
		{
			Name: "time",
			Type: "datetime64(3)",
		},
		{
			Name:    "_tp_time",
			Type:    "datetime64(3)",
			Default: "time",
		},
	},
	TTLExpression:          DefaultTTL,
	LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
	LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
}

var TripStream = timeplus.StreamDef{
	Name: "trips",
	Columns: []timeplus.ColumnDef{
		{
			Name: "pay_type",
			Type: "string",
		},
		{
			Name: "start_time",
			Type: "datetime64(3)",
		},
		{
			Name: "start_lon",
			Type: "float",
		},
		{
			Name: "start_lat",
			Type: "float",
		},
		{
			Name: "end_lon",
			Type: "float",
		},
		{
			Name: "end_lat",
			Type: "float",
		},
		{
			Name: "distance",
			Type: "float",
		},
		{
			Name: "amount",
			Type: "decimal(10, 2)",
		},
		{
			Name: "tid",
			Type: "string",
		},
		{
			Name: "end_time",
			Type: "datetime64(3)",
		},
		{
			Name: "bid",
			Type: "string",
		},
		{
			Name:    "_tp_time",
			Type:    "datetime64(3)",
			Default: "end_time",
		},
	},
	TTLExpression:          DefaultTTL,
	LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
	LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
}

var CarInfoView = timeplus.View{
	Name:         "car_info",
	Query:        "select * from table(dim_car_info)",
	Materialized: false,
}

var UserInfoView = timeplus.View{
	Name:         "user_info",
	Query:        "select * from table(dim_user_info)",
	Materialized: false,
}

var RevenueView = timeplus.View{
	Name:         "today_revenue",
	Query:        "select sum(amount) from trips where end_time > today()",
	Materialized: true,
}

type TimeplusSink struct {
	client    *timeplus.TimeplusClient
	producers map[string]*TimeplusStreamProducer
}

func NewTimeplusSink(properties map[string]any) (*TimeplusSink, error) {
	address, err := utils.GetWithDefault(properties, "address", "http://localhost:8000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	apikey, err := utils.GetWithDefault(properties, "apikey", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	tenant, err := utils.GetWithDefault(properties, "tenant", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	interval, err := utils.GetIntWithDefault(properties, "interval", 200)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	server := timeplus.NewCient(address, tenant, apikey)

	producerInterval := time.Duration(interval) * time.Millisecond
	producers := make(map[string]*TimeplusStreamProducer)
	producers["car_live_data"] = NewTimeplusStreamProducer(server, "car_live_data", producerInterval)
	producers["trips"] = NewTimeplusStreamProducer(server, "trips", producerInterval)
	producers["bookings"] = NewTimeplusStreamProducer(server, "bookings", producerInterval)

	return &TimeplusSink{
		client:    server,
		producers: producers,
	}, nil
}

func (s *TimeplusSink) Init() error {
	if err := s.initStream(DimCarStreamDef); err != nil {
		log.Logger().Warnf("DimCarStream failed to init")
		return err
	}

	if err := s.initStream(DimUserStreamDef); err != nil {
		log.Logger().Warnf("DimUserStream failed to init")
		return err
	}

	if err := s.initStream(BookingStreamDef); err != nil {
		log.Logger().Warnf("BookingStream failed to init")
		return err
	}

	if err := s.initStream(CarStream); err != nil {
		log.Logger().Warnf("CarStream failed to init")
		return err
	}

	if err := s.initStream(TripStream); err != nil {
		log.Logger().Warnf("TripStream failed to init")
		return err
	}

	if err := s.initView(CarInfoView); err != nil {
		return err
	}

	if err := s.initView(UserInfoView); err != nil {
		return err
	}

	if err := s.initView(RevenueView); err != nil {
		return err
	}

	return nil
}

func (s *TimeplusSink) initStream(streamDef timeplus.StreamDef) error {
	log.Logger().Infof("Calling Stream Init")

	if s.client.ExistStream(streamDef.Name) {
		if streamDef.Name == DimCarStreamDef.Name || streamDef.Name == DimUserStreamDef.Name {
			log.Logger().Warnf("stream %s already exist, no need to delete and recreate", streamDef.Name)
			if err := s.client.DeleteStream(streamDef.Name); err != nil {
				return err
			}
			return s.client.CreateStream(streamDef)
		} else {
			log.Logger().Warnf("stream %s already exist, no need to create", streamDef.Name)
			return nil
		}
	}
	return s.client.CreateStream(streamDef)
}

func (s *TimeplusSink) initView(view timeplus.View) error {
	if s.client.ExistView(view.Name) {
		log.Logger().Warnf("stream %s already exist, no need to create", view.Name)
		return nil
	}
	return s.client.CreateView(view)
}

func dimCarsToIngestData(cars []*common.DimCar) timeplus.IngestData {
	ingestData := timeplus.IngestData{}
	ingestData.Columns = common.GetDimCarHeader()
	ingestData.Data = make([][]any, len(cars))
	for i, car := range cars {
		ingestData.Data[i] = car.ToRow()
	}
	return ingestData
}

func (s *TimeplusSink) InitCars(cars []*common.DimCar) error {
	ingestData := dimCarsToIngestData(cars)
	payload := &timeplus.IngestPayload{
		Data:   ingestData,
		Stream: DimCarStreamDef.Name,
	}
	if err := s.client.InsertData(payload); err != nil {
		log.Logger().Errorf("failed to initialize data to stream %s, %s", DimCarStreamDef.Name, err)
		return err
	}
	return nil
}

func dimUsersToIngestData(users []*common.DimUser) timeplus.IngestData {
	ingestData := timeplus.IngestData{}
	ingestData.Columns = common.GetDimUserHeader()
	ingestData.Data = make([][]any, len(users))
	for i, user := range users {
		ingestData.Data[i] = user.ToRow()
	}
	return ingestData
}

func (s *TimeplusSink) InitUsers(users []*common.DimUser) error {
	ingestData := dimUsersToIngestData(users)
	payload := &timeplus.IngestPayload{
		Data:   ingestData,
		Stream: DimUserStreamDef.Name,
	}
	if err := s.client.InsertData(payload); err != nil {
		log.Logger().Errorf("failed to initialize data to stream %s, %s", DimUserStreamDef.Name, err)
		return err
	}
	return nil
}

func (s *TimeplusSink) Send(event map[string]any, stream string, timeCol string) error {
	if p, ok := s.producers[stream]; ok {
		p.produce(event)
	} else {
		log.Logger().Errorf("no such stream %s", stream)
	}
	return nil
}
