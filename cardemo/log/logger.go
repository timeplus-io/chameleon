package log

import (
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

var timeFormat = "2006-01-02 15:04:05.000 -0700"

var (
	logger *logrus.Logger
)

func init() {
	logger = nil
}

func Init(debug bool) {
	if debug {
		initDebug()
	} else {
		initProduction()
	}
}

func initDebug() {
	logger = logrus.New()
	logger.SetReportCaller(true)
	logger.SetLevel(logrus.DebugLevel)
	logger.SetOutput(os.Stdout)
}

func initProduction() {
	logger = logrus.New()
	logger.SetReportCaller(true)

	var formatter logrus.Formatter
	if viper.GetString("log-format") == "json" {
		jsonFormatter := new(logrus.JSONFormatter)
		jsonFormatter.TimestampFormat = timeFormat
		formatter = jsonFormatter
	} else {
		textFormatter := new(logrus.TextFormatter)
		textFormatter.TimestampFormat = timeFormat
		formatter = textFormatter
	}
	logger.SetFormatter(formatter)

	var level logrus.Level
	if l, err := logrus.ParseLevel(viper.GetString("log-level")); err != nil {
		fmt.Printf("failed to parse log level: %s\n", err)
		level = logrus.InfoLevel
	} else {
		level = l
	}
	fmt.Printf("logger initialization with level %d\n", level)
	logger.SetLevel(level)

	logPath := viper.GetString("log-file-path")
	var writer io.Writer = os.Stdout
	if len(logPath) > 0 {
		fmt.Printf("logger initialization with file %s\n", logPath)
		writer = io.MultiWriter(os.Stdout, &lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    100, // megabytes
			MaxBackups: 5,
			MaxAge:     10,   //days
			Compress:   true, // disabled by default
		})

		if file, err := os.OpenFile(logPath+".panic", os.O_CREATE|os.O_WRONLY, 0666); err != nil {
			fmt.Println("failed to log panic into file")
		} else {
			handlePanicLoggingWithFile(file)
		}
	}
	logger.SetOutput(writer)
}

func Logger() *logrus.Logger {
	if logger == nil {
		Init(false)
	}
	return logger
}
