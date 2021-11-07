package athenaconv

import (
	"fmt"
	"log"
)

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

var logLevel LogLevel = LogLevelWarn

// SetLogLevel overrides logLevel for athenaconv library, default is WARN
func SetLogLevel(lv LogLevel) {
	logLevel = lv
}

func LogDebug(format string, v ...interface{}) {
	if logLevel <= LogLevelDebug {
		format = fmt.Sprintf("athenaconv.debug: %s", format)
		log.Printf(format, v...)
	}
}

func LogInfo(format string, v ...interface{}) {
	if logLevel <= LogLevelInfo {
		format = fmt.Sprintf("athenaconv.info: %s", format)
		log.Printf(format, v...)
	}
}

func LogWarn(format string, v ...interface{}) {
	if logLevel <= LogLevelWarn {
		format = fmt.Sprintf("athenaconv.warn: %s", format)
		log.Printf(format, v...)
	}
}
