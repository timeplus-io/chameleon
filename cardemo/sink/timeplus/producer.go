package timeplus

import (
	"sync"
	"time"

	"github.com/timeplus-io/chameleon/cardemo/log"

	timeplus "github.com/timeplus-io/go-client/client"
)

type TimeplusStreamProducer struct {
	client   *timeplus.TimeplusClient
	stream   string
	queue    []map[string]any
	interval time.Duration
	lock     sync.Mutex
}

func NewTimeplusStreamProducer(server *timeplus.TimeplusClient, stream string, interval time.Duration) *TimeplusStreamProducer {
	producer := &TimeplusStreamProducer{
		client:   server,
		stream:   stream,
		queue:    make([]map[string]any, 0),
		interval: interval,
		lock:     sync.Mutex{},
	}

	go producer.start()
	return producer
}

func (p *TimeplusStreamProducer) produce(event map[string]any) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.queue = append(p.queue, event)
}

func (p *TimeplusStreamProducer) send() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.queue) == 0 {
		return
	}

	event0 := p.queue[0]
	ingestData := timeplus.IngestData{}
	header := make([]string, len(event0))
	data := make([][]any, len(p.queue))

	i := 0
	for key := range event0 {
		header[i] = key
		i++
	}

	for index, event := range p.queue {
		data[index] = make([]any, len(header))
		i := 0
		for _, key := range header {
			data[index][i] = event[key]
			i++
		}
	}

	ingestData.Columns = header
	ingestData.Data = data

	p.queue = make([]map[string]any, 0)

	payload := timeplus.IngestPayload{
		Data:   ingestData,
		Stream: p.stream,
	}

	go func() {
		if err := p.client.InsertData(payload); err != nil {
			log.Logger().Error(err)
		}
	}()

}

func (p *TimeplusStreamProducer) start() {
	for {
		p.send()
		time.Sleep(p.interval)
	}

}

func (p *TimeplusStreamProducer) stop() {

}
