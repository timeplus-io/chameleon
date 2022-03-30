package source

import "github.com/timeplus-io/chameleon/generator/common"

type Source interface {
	Start()
	Stop()
	Read() []common.Event
	IsFinished() bool
}
