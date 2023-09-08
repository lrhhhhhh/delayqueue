package log

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

var defaultLogger *zap.Logger
var defaultLevel string = "INFO"
var defaultEncoding string = "json"

func getLogger() *zap.Logger {
	if defaultLogger == nil {
		logger, err := newConsoleLogger(defaultLevel, defaultEncoding)
		if err != nil {
			panic(err)
		}
		defaultLogger = logger
	}
	return defaultLogger.WithOptions(zap.AddCallerSkip(1))
}

func newConsoleLogger(levelName string, encoding string) (*zap.Logger, error) {
	js := fmt.Sprintf(`{
      		"level": "%s",
            "encoding": "%s",
      		"outputPaths": ["stdout"],
            "errorOutputPaths": ["stdout"]
		}`, levelName, encoding)

	var config zap.Config
	err := json.Unmarshal([]byte(js), &config)
	if err != nil {
		return nil, err
	}

	timeFormatter := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
	}

	config.EncoderConfig = zap.NewProductionEncoderConfig()
	config.EncoderConfig.EncodeTime = timeFormatter
	config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return config.Build()
}

func Debug(msg string, fields ...zap.Field) {
	getLogger().Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	getLogger().Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	getLogger().Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	getLogger().Error(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	getLogger().Panic(msg, fields...)
}

func Debugf(format string, v ...interface{}) {
	getLogger().Debug(fmt.Sprintf(format, v...))
}

func Infof(format string, v ...interface{}) {
	getLogger().Info(fmt.Sprintf(format, v...))
}

func Warnf(format string, v ...interface{}) {
	getLogger().Warn(fmt.Sprintf(format, v...))
}

func Errorf(format string, v ...interface{}) {
	getLogger().Error(fmt.Sprintf(format, v...))
}
