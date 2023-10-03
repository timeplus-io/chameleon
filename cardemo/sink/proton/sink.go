package proton

import (
	"fmt"
	"time"

	"github.com/timeplus-io/chameleon/cardemo/common"
	"github.com/timeplus-io/chameleon/cardemo/log"
	"github.com/timeplus-io/chameleon/cardemo/utils"
)

const DefaultTTL = "to_datetime(_tp_time) + INTERVAL 30 DAY"
const DefaultLogStoreRetentionBytes = 604800000
const DefaultLogStoreRetentionMS = 1342177280

var DimCarStreamDef = StreamDef{
	Name: "dim_car_info",
	Columns: []ColumnDef{
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
	TTLExpression: DefaultTTL,
}

var DimUserStreamDef = StreamDef{
	Name: "dim_user_info",
	Columns: []ColumnDef{
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
	TTLExpression: DefaultTTL,
}

var BookingStreamDef = StreamDef{
	Name: "bookings",
	Columns: []ColumnDef{
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
	TTLExpression: DefaultTTL,
}

var CarStream = StreamDef{
	Name: "car_live_data",
	Columns: []ColumnDef{
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
	TTLExpression: DefaultTTL,
}

var TripStream = StreamDef{
	Name: "trips",
	Columns: []ColumnDef{
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
	TTLExpression: DefaultTTL,
}

var CarInfoView = ViewDef{
	Name:         "car_info",
	Query:        "select * from table(dim_car_info)",
	Materialized: false,
}

var UserInfoView = ViewDef{
	Name:         "user_info",
	Query:        "select * from table(dim_user_info)",
	Materialized: false,
}

var RevenueView = ViewDef{
	Name:         "today_revenue",
	Query:        "select sum(amount) from trips where end_time > today()",
	Materialized: true,
}

type ProtonSink struct {
	client    *Client
	producers map[string]*ProtonStreamProducer
}

func NewProtonSink(properties map[string]any) (*ProtonSink, error) {

	interval, err := utils.GetIntWithDefault(properties, "interval", 200)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	portRest, err := utils.GetIntWithDefault(properties, "rest_port", 3218)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	portTCP, err := utils.GetIntWithDefault(properties, "tcp_port", 8463)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	user, err := utils.GetWithDefault(properties, "user", "default")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	password, err := utils.GetWithDefault(properties, "password", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	driverConfig := NewConfig(host, user, password, portTCP)
	driver := NewEngine(driverConfig)

	server := NewClient("", host, portRest, user, password, driver, driver)

	producerInterval := time.Duration(interval) * time.Millisecond
	producers := make(map[string]*ProtonStreamProducer)
	producers["car_live_data"] = NewProtonStreamProducer(server, "car_live_data", producerInterval)
	producers["trips"] = NewProtonStreamProducer(server, "trips", producerInterval)
	producers["bookings"] = NewProtonStreamProducer(server, "bookings", producerInterval)

	return &ProtonSink{
		client:    server,
		producers: producers,
	}, nil
}

func (s *ProtonSink) Init() error {
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

func (s *ProtonSink) initStream(streamDef StreamDef) error {
	log.Logger().Infof("Calling Stream Init")

	streamStorageConfig := StreamStorageConfig{
		RetentionBytes: DefaultLogStoreRetentionBytes,
		RetentionMS:    DefaultLogStoreRetentionMS,
	}

	if s.client.ExistStream(streamDef.Name) {
		if streamDef.Name == DimCarStreamDef.Name {
			if err := s.client.DeleteStreamView(CarInfoView.Name); err != nil {
				log.Logger().Errorf("failed to delete view %s, %s", CarInfoView.Name, err.Error())
				return err
			}
		}

		if streamDef.Name == DimUserStreamDef.Name {
			if err := s.client.DeleteStreamView(UserInfoView.Name); err != nil {
				log.Logger().Errorf("failed to delete view %s, %s", UserInfoView.Name, err.Error())
				return err
			}
		}

		if streamDef.Name == DimCarStreamDef.Name || streamDef.Name == DimUserStreamDef.Name {
			log.Logger().Warnf("recreate stream %s", streamDef.Name)
			if err := s.client.DeleteStreamView(streamDef.Name); err != nil {
				log.Logger().Errorf("failed to delete existing stream %s, %s", streamDef.Name, err.Error())
				return err
			}
			return s.client.CreateStream(streamDef, streamStorageConfig)
		} else {
			log.Logger().Warnf("stream %s already exist, no need to create", streamDef.Name)
			return nil
		}
	}
	return s.client.CreateStream(streamDef, streamStorageConfig)
}

func (s *ProtonSink) initView(view ViewDef) error {
	if s.client.ExistView(view.Name) {
		log.Logger().Warnf("stream %s already exist, no need to create", view.Name)
		return nil
	}
	return s.client.CreateView(view)
}

func dimCarsToIngestData(cars []*common.DimCar) IngestData {
	ingestData := IngestData{}
	ingestData.Columns = common.GetDimCarHeader()
	ingestData.Data = make([]IngestDataRow, len(cars))
	for i, car := range cars {
		ingestData.Data[i] = car.ToRow()
	}
	return ingestData
}

func (s *ProtonSink) InitCars(cars []*common.DimCar) error {
	ingestData := dimCarsToIngestData(cars)

	if _, err := s.client.IngestEvent(ingestData, DimCarStreamDef.Name); err != nil {
		log.Logger().Errorf("failed to initialize data to stream %s, %s", DimCarStreamDef.Name, err)
		return err
	}
	return nil
}

func dimUsersToIngestData(users []*common.DimUser) IngestData {
	ingestData := IngestData{}
	ingestData.Columns = common.GetDimUserHeader()
	ingestData.Data = make([]IngestDataRow, len(users))
	for i, user := range users {
		ingestData.Data[i] = user.ToRow()
	}
	return ingestData
}

func (s *ProtonSink) InitUsers(users []*common.DimUser) error {
	ingestData := dimUsersToIngestData(users)
	if _, err := s.client.IngestEvent(ingestData, DimUserStreamDef.Name); err != nil {
		log.Logger().Errorf("failed to initialize data to stream %s, %s", DimUserStreamDef.Name, err)
		return err
	}
	return nil
}

func (s *ProtonSink) Send(event map[string]any, stream string, timeCol string) error {
	if p, ok := s.producers[stream]; ok {
		p.produce(event)
	} else {
		log.Logger().Errorf("no such stream %s", stream)
	}
	return nil
}
