package demo

import (
	"math"
)

func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h))
}

// Note: following code are duplicated from source.Connection
// func isInt(val float64) bool {
// 	return val == float64(int(val))
// }

// func getInferredProtonTypes(in reflect.Type, val any) string {
// 	switch in.Kind() {
// 	case reflect.Float32:
// 		value := val.(float32)
// 		if isInt(float64(value)) {
// 			return "uint32"
// 		}
// 		return "float64"
// 	case reflect.Float64:
// 		value := val.(float64)
// 		if isInt(value) {
// 			return "uint32"
// 		}
// 		return "float64"
// 	case reflect.String:
// 		return "string"
// 	case reflect.Bool:
// 		return "uint8"
// 	case reflect.Int:
// 		return "uint32"
// 	case reflect.Int64: // check if the int got parsed right then
// 		return "datetime64(3)"
// 	case reflect.Struct:
// 		switch val.(type) {
// 		case time.Time:
// 			return "datetime64(3)"
// 		default:
// 			return "string"
// 		}
// 	default:
// 		log.Logger().Warnf("cannot find the type to infer %v %v %T", in, val, val)
// 		return "string"
// 	}
// }

// func AutoCreateStream(event map[string]any, stream string, protonServer proton.Proton, timeColumn string) error {
// 	if event == nil {
// 		return fmt.Errorf("got empty event")
// 	}

// 	var streamDef proton.StreamDef
// 	streamDef.Name = stream
// 	dataRow := event

// 	if exist, err := protonServer.StreamExist(streamDef.Name); err != nil {
// 		return fmt.Errorf("failed to read stream status")
// 	} else {
// 		if exist {
// 			log.Logger().Infof("stream %s already exist", streamDef.Name)
// 			return nil
// 		}

// 		log.Logger().Infof("stream %s does not exist ", streamDef.Name)
// 	}

// 	timestampCol := ""
// 	timestampColType := ""
// 	if timeColumn != "" {
// 		timestampCol = timeColumn
// 	}
// 	log.Logger().Debugf("Timestamp column is %s", timestampCol)

// 	columnDefs := make([]proton.ColumnDef, len(dataRow))
// 	log.Logger().Debugf("event used for stream creation is %v", dataRow)
// 	index := 0
// 	for k := range dataRow {
// 		columnDefs[index].Name = k
// 		if colType := viper.GetString(fmt.Sprintf("cardemo.type.%s.%s", stream, columnDefs[index].Name)); colType != "" {
// 			log.Logger().Debugf("found col definition %s, %s , %v", stream, columnDefs[index].Name, colType)
// 			columnDefs[index].Type = colType
// 		} else {
// 			columnDefs[index].Type = getInferredProtonTypes(reflect.TypeOf(dataRow[k]), dataRow[k])
// 		}

// 		if columnDefs[index].Name == timestampCol {
// 			timestampColType = columnDefs[index].Type
// 		}
// 		log.Logger().Debugf("row type is %v ; inferred type is %v", reflect.TypeOf(dataRow[k]), columnDefs[index].Type)
// 		index++
// 	}

// 	log.Logger().Debugf("Creating %s with column def %v", streamDef.Name, columnDefs)
// 	streamDef.Columns = columnDefs
// 	if timestampCol != "" {
// 		if timestampColType == "string" {
// 			streamDef.EventTimeColumn = fmt.Sprintf("parse_datetime64_best_effort_or_zero(%s)", timestampCol)
// 		} else {
// 			streamDef.EventTimeColumn = timestampCol
// 		}
// 	}

// 	log.Logger().Debugf("stream def is %v", streamDef)

// 	tableTTL := viper.GetInt("cardemo.app.storage.table.ttl")
// 	streamTTL := viper.GetInt("cardemo.app.storage.stream.ttl")
// 	streamSizeLimit := viper.GetInt("cardemo.app.storage.stream.sizeGiB")
// 	log.Logger().Debugf("set table ttl %d hours, stream ttl %d hours", tableTTL, streamTTL)
// 	streamDef.TTLExpression = fmt.Sprintf("to_datetime(_tp_time) + INTERVAL %d HOUR", tableTTL)

// 	// remove the ddl from dim table
// 	if strings.HasPrefix(stream, "dim_") {
// 		streamDef.TTLExpression = ""
// 	}
// 	streamStorageConfig := proton.NewStreamStorageConfig(streamSizeLimit*1024*1024*1024, streamTTL*1000*60*60)

// 	if err := protonServer.CreateStream(streamDef, *streamStorageConfig); err != nil {
// 		return err
// 	}

// 	return nil
// }
