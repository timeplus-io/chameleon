package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"os"
	"strconv"
	"strings"

	"github.com/timeplus-io/chameleon/tsbs/common"
	"github.com/timeplus-io/chameleon/tsbs/loader"
	"github.com/timeplus-io/chameleon/tsbs/log"

	timeplus "github.com/timeplus-io/go-client/client"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func Run(_ *cobra.Command, _ []string) error {
	dataFile := viper.GetString("source")
	metrics, payloads := loadDataFile(dataFile)
	log.Logger().Infof("got metrcis %v", metrics)
	log.Logger().Infof("got payloads %d", len(payloads))

	metrics = processExtraTags(metrics, payloads)

	timeplusAddress := viper.GetString("timeplus-address")
	tmieplusApiKey := viper.GetString("timeplus-apikey")
	server := timeplus.NewCient(timeplusAddress, tmieplusApiKey)

	realtimeIngest := viper.GetBool("realtime-ingest")
	var dataloader loader.DataLoader
	if viper.GetString("metrics-schema") == "single" {
		name := viper.GetString("metrics-name")
		dataloader = loader.NewSingleStreamStoreLoader(server, metrics, name, realtimeIngest)
	} else {
		dataloader = loader.NewMultipleStreamStoreLoader(server, metrics, realtimeIngest)
	}

	if !viper.GetBool("skip-create-streams") {
		dataloader.DeleteStreams()
		if err := dataloader.CreateStreams(); err != nil {
			log.Logger().WithError(err).Warn("failed to create stream")
		}
	}

	dataloader.Ingest(payloads)
	return nil
}

func buildTags(tagLine string) []common.Tag {
	tags := []common.Tag{}
	tokens := strings.Split(tagLine, ",")
	for _, token := range tokens[1:] {
		tag := strings.Split(token, " ")

		tagStrcut := common.Tag{
			Name: tag[0],
			Type: tag[1],
		}
		tags = append(tags, tagStrcut)
	}
	return tags
}

func buildMetric(measureLine string, tags []common.Tag) common.Metric {
	metric := common.Metric{}
	tokens := strings.Split(measureLine, ",")
	metric.Name = tokens[0]
	metric.Values = []common.MeausreValue{}
	for _, token := range tokens[1:] {
		mv := common.MeausreValue{
			Name: token,
			Type: "float64",
		}
		metric.Values = append(metric.Values, mv)
	}

	metric.Tags = tags
	return metric
}

func buildPayload(tagline string, measureLine string) common.Payload {
	payload := common.Payload{}

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

func loadDataFile(file string) ([]common.Metric, []common.Payload) {
	log.Logger().Debugf("loading file %s", file)
	metrics := []common.Metric{}
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

	payloads := []common.Payload{}
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

func processExtraTags(metrics []common.Metric, payloads []common.Payload) []common.Metric {
	metricsCopy := make([]common.Metric, 0)
	metricsCopy = append(metricsCopy, metrics...)

	for _, payload := range payloads {
		index := common.FindMetricIndexByName(metricsCopy, payload.Name)

		for _, tag := range payload.Tags {
			if !contains(metricsCopy[index].Tags, tag) {
				log.Logger().Debugf("find new tags %s", tag)
				newTag := common.Tag{
					Name: tag,
					Type: "string",
				}
				metricsCopy[index].Tags = append(metricsCopy[index].Tags, newTag)
			}
		}

	}
	return metricsCopy
}

func contains(tags []common.Tag, tagName string) bool {
	for _, tag := range tags {
		if tag.Name == tagName {
			return true
		}
	}
	return false
}
