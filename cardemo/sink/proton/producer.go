package proton

import (
	"sync"
	"time"

	"github.com/timeplus-io/chameleon/cardemo/log"
)

type ProtonStreamProducer struct {
	client   *Client
	stream   string
	queue    []map[string]any
	interval time.Duration
	lock     sync.Mutex
}

func NewProtonStreamProducer(server *Client, stream string, interval time.Duration) *ProtonStreamProducer {
	producer := &ProtonStreamProducer{
		client:   server,
		stream:   stream,
		queue:    make([]map[string]any, 0),
		interval: interval,
		lock:     sync.Mutex{},
	}

	go producer.start()
	return producer
}

func (p *ProtonStreamProducer) produce(event map[string]any) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.queue = append(p.queue, event)
}

func (p *ProtonStreamProducer) send() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.queue) == 0 {
		return
	}

	event0 := p.queue[0]
	ingestData := IngestData{}
	header := make([]string, len(event0))
	data := make([]IngestDataRow, len(p.queue))

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

	// payload := &timeplus.IngestPayload{
	// 	Data:   ingestData,
	// 	Stream: p.stream,
	// }

	go func() {
		if _, err := p.client.IngestEvent(ingestData, p.stream); err != nil {
			log.Logger().Error(err)
		}
	}()

}

func (p *ProtonStreamProducer) start() {
	for {
		p.send()
		time.Sleep(p.interval)
	}

}

func (p *ProtonStreamProducer) stop() {

}
