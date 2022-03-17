package models

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

type Log struct {
	Data       string    `db:"_raw"`
	Sourcetype string    `db:"sourcetype"`
	IndexTime  time.Time `db:"_index_time"`
}

func GenerateLogRecords(filename string, timestampRegex string, lineBreakerRegex string, typ string, results chan *Log) error {
	tsRegex, err := regexp.Compile(timestampRegex)
	if err != nil {
		return err
	}

	lbRegex, err := regexp.Compile(lineBreakerRegex)
	if err != nil {
		return err
	}

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader
	r = f

	if strings.HasSuffix(filename, ".gz") {
		r, err = gzip.NewReader(f)
		if err != nil {
			return err
		}
	}

	br := bufio.NewReader(r)
	var aggr [][]byte
	var done bool
	var record *Log

	for {
		line, _, err := br.ReadLine()
		if err != nil {
			if err != io.EOF {
				return err
			}

			record, aggr, done = handleLine(line, aggr, tsRegex, lbRegex, typ)
			if done {
				results <- record
			}
			results <- nil
			return nil
		}

		record, aggr, done = handleLine(line, aggr, tsRegex, lbRegex, typ)
		if done {
			results <- record
		}
	}
}

func handleLine(line []byte, aggr [][]byte, tsRegex *regexp.Regexp, lbRegex *regexp.Regexp, typ string) (*Log, [][]byte, bool) {
	line = replaceTimestamp(line, tsRegex)

	if lbRegex.Find(line) != nil || line == nil {
		if len(aggr) > 0 {
			// project preivous cached line as a single event
			log := &Log{
				Data:       string(bytes.Join(aggr, []byte("\n"))),
				IndexTime:  time.Now(),
				Sourcetype: typ,
			}
			if line != nil {
				aggr = [][]byte{line}
			} else {
				aggr = nil
			}
			return log, aggr, true
		}
	}

	aggr = append(aggr, line)
	return nil, aggr, false
}

func replaceTimestamp(line []byte, tsRegex *regexp.Regexp) []byte {
	// FIXME
	return line
}
