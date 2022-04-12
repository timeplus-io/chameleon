package source

import (
	rxgo "github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/generator/common"
)

type Source interface {
	Start()
	Stop()
	Read() []common.Event
	GetStreams() []rxgo.Observable
	IsFinished() bool
	GetFields() []common.Field
}
