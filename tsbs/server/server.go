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
	Tags   []Tag
}

type Payload struct {
	Name      string
	Timestamp string
	Data      []interface{}
	Tags      []string
}

type MeausreValue struct {
	Name string
	Type string
}

type Tag struct {
	Name string
	Type string
}

func (m *Metric) getHeader() []string {
	header := []string{}

	header = append(header, "ts")

	for _, tag := range m.Tags {
		header = append(header, tag.Name)
	}

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

	metrics = processExtraTags(metrics, payloads)

	timeplusAddress := viper.GetString("timeplus-address")
	tmieplusApiKey := viper.GetString("timeplus-apikey")
	server := timeplus.NewNeutronServer(timeplusAddress, tmieplusApiKey)

	if !viper.GetBool("skip-create-streams") {
		deleteStreams(server, metrics)

		if err := createStreams(server, metrics); err != nil {
			log.Logger().WithError(err).Warn("failed to create stream")
		}
	}

	//ingest(server, metrics, payloads)

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
			Name: tag.Name,
			Type: "string", // use string type for all tag for now
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
	for index, metric := range metrics {
		if metric.Name == name {
			return &metrics[index]
		}
	}

	log.Logger().Fatalf("not such metrics %s", name)
	return nil
}

func findMetricIndexByName(metrics []Metric, name string) int {
	for index, metric := range metrics {
		if metric.Name == name {
			return index
		}
	}

	log.Logger().Fatalf("not such metrics %s", name)
	return -1
}

func ingest(server *timeplus.NeutronServer, metrics []Metric, payloads []Payload) {
	for _, payload := range payloads {
		metricName := payload.Name
		metric := findMetricByName(metrics, metricName)
		headers := metric.getHeader()

		data := make([][]interface{}, 1)
		data[0] = payload.Data

		load := timeplus.IngestPayload{
			Stream: metricName,
			Data: timeplus.IngestData{
				Columns: headers,
				Data:    data,
			},
		}

		if err := server.InsertData(load); err != nil {
			log.Logger().WithError(err).Fatalf("failed to ingest")
		}
	}
}

func buildTags(tagLine string) []Tag {
	tags := []Tag{}
	tokens := strings.Split(tagLine, ",")
	for _, token := range tokens[1:] {
		tag := strings.Split(token, " ")

		tagStrcut := Tag{
			Name: tag[0],
			Type: tag[1],
		}
		tags = append(tags, tagStrcut)
	}
	return tags
}

func buildMetric(measureLine string, tags []Tag) Metric {
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
	payload := Payload{}

	tags := strings.Split(tagline, ",")
	measures := strings.Split(measureLine, ",")

	payload.Name = measures[0]
	payload.Timestamp = measures[1]
	payload.Data = []interface{}{}
	payload.Data = append(payload.Data, payload.Timestamp)

	payload.Tags = []string{}

	for _, tag := range tags[1:] {
		tagValue := strings.Split(tag, "=")
		payload.Data = append(payload.Data, tagValue[1])
		payload.Tags = append(payload.Tags, tagValue[0])
	}

	for _, measure := range measures[2:] {
		if len(measure) == 0 {
			payload.Data = append(payload.Data, nil)
		} else {
			if f, err := strconv.ParseFloat(measure, 64); err != nil {
				log.Logger().Debugf("build payload tagline : %s", tagline)
				log.Logger().Debugf("build payload measureLine : %s", measureLine)
				log.Logger().Fatalf("failed to parse float value %s ", measure)
			} else {
				payload.Data = append(payload.Data, f)
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

func processExtraTags(metrics []Metric, payloads []Payload) []Metric {
	metricsCopy := make([]Metric, 0)
	metricsCopy = append(metricsCopy, metrics...)

	for _, payload := range payloads {
		index := findMetricIndexByName(metricsCopy, payload.Name)

		for _, tag := range payload.Tags {
			if !contains(metricsCopy[index].Tags, tag) {
				log.Logger().Infof("find new tags %s", tag)
				newTag := Tag{
					Name: tag,
					Type: "string",
				}
				log.Logger().Infof("the metric before %v", metricsCopy[index])
				metricsCopy[index].Tags = append(metricsCopy[index].Tags, newTag)
				log.Logger().Infof("the metric after %v", metricsCopy[index])
				log.Logger().Infof("the metrics after %v", metricsCopy)

			}
		}

	}
	log.Logger().Infof("processed metrics is: %v", metricsCopy)
	return metricsCopy
}

func contains(tags []Tag, tagName string) bool {
	for _, tag := range tags {
		if tag.Name == tagName {
			return true
		}
	}
	return false
}
