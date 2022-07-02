package loader

import (
	"github.com/timeplus-io/chameleon/tsbs/common"
)

type DataLoader interface {
	DeleteStreams()
	CreateStreams() error

	Ingest(payloads []common.Payload)
}
