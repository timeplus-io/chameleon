package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"os"
	"strconv"
	"strings"

	"github.com/timeplus-io/chameleon/tsbs/log"
	"github.com/timeplus-io/chameleon/tsbs/timeplus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Metric struct {
	Name   string
	Values []MeausreValue
	Tags   []string
}

type Payload []interface{}

type MeausreValue struct {
	Name string
	Type string
}

func (m *Metric) getHeader() []string {
	header := []string{}

	header = append(header, "ts")
	header = append(header, m.Tags...)

	for _, measure := range m.Values {
		header = append(header, measure.Name)
	}

	return header
}

func Run(_ *cobra.Command, _ []string) error {
	dataFile := viper.GetString("source")

	metrics, payloads := loadDataFile(dataFile)
	log.Logger().Infof("got metrcis %v", metrics)
	log.Logger().Infof("got payloads %d", len(payloads))
	log.Logger().Infof("got payloads sample %v", payloads[123])

	timeplusAddress := viper.GetString("timeplus-address")
	tmieplusApiKey := viper.GetString("timeplus-apikey")
	server := timeplus.NewNeutronServer(timeplusAddress, tmieplusApiKey)

	if viper.GetBool("clean-streams") {
		deleteStreams(server, metrics)
	}

	if !viper.GetBool("skip-create-streams") {
		if err := createStreams(server, metrics); err != nil {
			log.Logger().WithError(err).Warn("failed to create stream")
		}
	}

	ingest(server, metrics, payloads)

	return nil
}

func deleteStreams(server *timeplus.NeutronServer, metrics []Metric) {
	for _, metric := range metrics {
		if err := server.DeleteStream(metric.Name); err != nil {
			log.Logger().WithError(err).Warnf("failed to delete stream %s", metric.Name)
		} else {
			log.Logger().Infof("stream %s deleted", metric.Name)
		}
	}
}

func createStreams(server *timeplus.NeutronServer, metrics []Metric) error {
	for _, metric := range metrics {
		if err := createStream(server, metric); err != nil {
			return err
		} else {
			log.Logger().Infof("stream %s created", metric.Name)
		}
	}
	return nil
}

func createStream(server *timeplus.NeutronServer, metric Metric) error {
	streamDef := timeplus.StreamDef{
		Name: metric.Name,
	}

	cols := []timeplus.ColumnDef{}
	timeCol := timeplus.ColumnDef{
		Name: "ts",
		Type: "string",
	}
	cols = append(cols, timeCol)

	for _, tag := range metric.Tags {
		col := timeplus.ColumnDef{
			Name: tag,
			Type: "string",
		}
		cols = append(cols, col)
	}

	for _, measure := range metric.Values {
		col := timeplus.ColumnDef{
			Name: measure.Name,
			Type: measure.Type,
		}
		cols = append(cols, col)
	}

	streamDef.Columns = cols
	return server.CreateStream(streamDef)
}

func findMetricByName(metrics []Metric, name string) *Metric {
	for _, metric := range metrics {
		if metric.Name == name {
			return &metric
		}
	}

	log.Logger().Fatalf("not such metrics %s", name)
	return nil
}

func ingest(server *timeplus.NeutronServer, metrics []Metric, payloads []Payload) {
	for _, payload := range payloads {
		metricName := payload[0].(string)
		metric := findMetricByName(metrics, metricName)
		headers := metric.getHeader()

		data := make([][]interface{}, 1)
		data[0] = payload[1:]

		load := timeplus.IngestPayload{
			Stream: metricName,
			Data: timeplus.IngestData{
				Columns: headers,
				Data:    data,
			},
		}

		if err := server.InsertData(load); err != nil {
			log.Logger().WithError(err).Warnf("failed to ingest")
		}
	}
}

func buildTags(tagLine string) []string {
	tags := []string{}
	tokens := strings.Split(tagLine, ",")
	for _, token := range tokens[1:] {
		tag := strings.Split(token, " ")[0]
		tags = append(tags, tag)
	}
	return tags
}

func buildMetric(measureLine string, tags []string) Metric {
	metric := Metric{}
	tokens := strings.Split(measureLine, ",")
	metric.Name = tokens[0]
	metric.Values = []MeausreValue{}
	for _, token := range tokens[1:] {
		mv := MeausreValue{
			Name: token,
			Type: "float64",
		}
		metric.Values = append(metric.Values, mv)
	}

	metric.Tags = tags
	return metric
}

func buildPayload(tagline string, measureLine string) Payload {
	log.Logger().Debugf("build payload tagline : %s", tagline)
	log.Logger().Debugf("build payload measureLine : %s", measureLine)
	payload := make(Payload, 0)

	tags := strings.Split(tagline, ",")
	measures := strings.Split(measureLine, ",")

	payload = append(payload, measures[0])
	payload = append(payload, measures[1])

	for _, tag := range tags[1:] {
		tagValue := strings.Split(tag, "=")[1]
		payload = append(payload, tagValue)
	}

	for _, measure := range measures[2:] {
		if len(measure) == 0 {
			payload = append(payload, nil)
		} else {
			if f, err := strconv.ParseFloat(measure, 64); err != nil {
				log.Logger().Fatalf("failed to parse float value %s ", measure)
			} else {
				payload = append(payload, f)
			}
		}

	}
	return payload
}

func loadDataFile(file string) ([]Metric, []Payload) {
	log.Logger().Debugf("loading file %s", file)
	metrics := []Metric{}
	dat, err := os.ReadFile(file)
	if err != nil {
		log.Logger().Fatal("failed to read file %s", file)
	}

	reader := bytes.NewReader(dat)
	zr, err := gzip.NewReader(reader)
	if err != nil {
		log.Logger().Fatal("failed to unzip file %s", file)
	}

	scanner := bufio.NewScanner(zr)
	scanner.Scan()
	tag := scanner.Text()
	tags := buildTags(tag)
	log.Logger().Debugf("the tags is %v", tags)

	for {
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		if len(line) == 0 {
			log.Logger().Debugf("end of schema")
			break
		}

		metric := buildMetric(line, tags)
		metrics = append(metrics, metric)
		log.Logger().Debugf("got one metric %v", metric)
	}

	payloads := []Payload{}
	for {
		if !scanner.Scan() {
			break
		}
		tagline := scanner.Text()

		if !scanner.Scan() {
			break
		}
		measureline := scanner.Text()

		payload := buildPayload(tagline, measureline)
		payloads = append(payloads, payload)
	}

	if err := zr.Close(); err != nil {
		log.Logger().WithError(err).Fatal()
	}

	return metrics, payloads
}
