package log

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

var timeFormat = "2006-01-02 15:04:05.000 -0700"

var (
	logger       *logrus.Logger
	accessLogger *logrus.Logger
)

func init() {
	logger = nil
	accessLogger = nil
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

	accessLogger = logrus.New()
	accessLogger.SetLevel(logrus.DebugLevel)
	accessLogger.SetOutput(os.Stdout)
}

func initProduction() {
	logger = logrus.New()
	logger.SetReportCaller(true)

	accessLogger = logrus.New()

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
	accessLogger.SetFormatter(formatter)

	var level logrus.Level
	if l, err := logrus.ParseLevel(viper.GetString("log-level")); err != nil {
		fmt.Printf("failed to parse log level: %s\n", err)
		level = logrus.InfoLevel
	} else {
		level = l
	}
	fmt.Printf("logger initialization with level %d\n", level)
	logger.SetLevel(level)
	accessLogger.SetLevel(level)

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

	accessLogPath := viper.GetString("access-log-file-path")
	writer = os.Stdout
	if len(accessLogPath) > 0 {
		fmt.Printf("access log logger initialization with file %s\n", accessLogPath)
		writer = &lumberjack.Logger{
			Filename:   accessLogPath,
			MaxSize:    100, // megabytes
			MaxBackups: 5,
			MaxAge:     10,   //days
			Compress:   true, // disabled by default
		}
	}
	accessLogger.SetOutput(writer)
}

func Logger() *logrus.Logger {
	if logger == nil {
		Init(false)
	}
	return logger
}

func LoggerHandler(notLogged ...string) gin.HandlerFunc {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknow"
	}

	var skip map[string]struct{}

	if length := len(notLogged); length > 0 {
		skip = make(map[string]struct{}, length)

		for _, p := range notLogged {
			skip[p] = struct{}{}
		}
	}

	return func(c *gin.Context) {
		// other handler can change c.Path so:
		path := c.Request.URL.Path
		start := time.Now()

		contextLogger := logger.WithFields(logrus.Fields{
			"hostname": hostname,
			"method":   c.Request.Method,
			"path":     path,
		})
		// contextLogger.Logger.SetOutput(writer)
		c.Set("logger", contextLogger)
		c.Next()

		stop := time.Since(start)
		latency := int(math.Ceil(float64(stop.Nanoseconds()) / 1000000.0))
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()
		clientUserAgent := c.Request.UserAgent()
		referer := c.Request.Referer()
		dataLength := c.Writer.Size()
		if dataLength < 0 {
			dataLength = 0
		}

		if _, ok := skip[path]; ok {
			return
		}

		entry := accessLogger.WithFields(logrus.Fields{
			"hostname":   hostname,
			"statusCode": statusCode,
			"latency":    latency,
			"clientIP":   clientIP,
			"method":     c.Request.Method,
			"path":       path,
			"referer":    referer,
			"dataLength": dataLength,
			"userAgent":  clientUserAgent,
		})

		if len(c.Errors) > 0 {
			entry.Error(c.Errors.ByType(gin.ErrorTypePrivate).String())
		} else {
			msg := fmt.Sprintf("%s - %s [%s] \"%s %s\" %d %d \"%s\" \"%s\" (%dms)", clientIP, hostname, time.Now().Format(timeFormat), c.Request.Method, path, statusCode, dataLength, referer, clientUserAgent, latency)
			if statusCode >= http.StatusInternalServerError {
				entry.Error(msg)
			} else if statusCode >= http.StatusBadRequest {
				entry.Warn(msg)
			} else {
				entry.Info(msg)
			}
		}
	}
}
