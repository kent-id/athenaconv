package athenaconv

import (
	"fmt"
	"log"
)

// LogLevel is data type for log level, use SetLogLevel to set logLevel for this package.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

var logLevel LogLevel = LogLevelWarn

// SetLogLevel overrides logLevel for athenaconv package, default is WARN.
func SetLogLevel(lv LogLevel) {
	logLevel = lv
	log.SetFlags(0)
}

// LogDebugf accepts log format and values and logs the message if logLevel is set to DEBUG.
func LogDebugf(format string, v ...interface{}) {
	if logLevel <= LogLevelDebug {
		format = fmt.Sprintf("athenaconv.debug: %s", format)
		log.Printf(format, v...)
	}
}

// LogDebugf accepts log format and values and logs the message if logLevel is set to DEBUG or INFO.
func LogInfof(format string, v ...interface{}) {
	if logLevel <= LogLevelInfo {
		format = fmt.Sprintf("athenaconv.info: %s", format)
		log.Printf(format, v...)
	}
}

// LogDebugf accepts log format and values and logs the message if logLevel is set to DEBUG, INFO, or WARN.
func LogWarnf(format string, v ...interface{}) {
	if logLevel <= LogLevelWarn {
		format = fmt.Sprintf("athenaconv.warn: %s", format)
		log.Printf(format, v...)
	}
}
