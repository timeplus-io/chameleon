package sink

func CreateSink(config Configuration) Sink {
	return NewConsoleSink()
}
