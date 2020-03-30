package log

import "go.uber.org/zap"

type Logger interface {
	Debug(msg string, keyAndValues...interface{})
	Info(msg string, keyAndValues...interface{})
	Warn(msg string, keyAndValues...interface{})
	Error(msg string, keyAndValues...interface{})
	Fatal(msg string, keyAndValues...interface{})
}

type ZapLogger struct {
	inner *zap.SugaredLogger
}

func NewZapLogger(log *zap.Logger) ZapLogger {
	return ZapLogger{inner: log.Sugar()}
}

func (l ZapLogger) Debug(msg string, keyAndValues...interface{}) {
	l.inner.Debug(msg, keyAndValues)
}

func (l ZapLogger) Info(msg string, keyAndValues...interface{}) {
	l.inner.Info(msg, keyAndValues)
}

func (l ZapLogger) Warn(msg string, keyAndValues...interface{}) {
	l.inner.Warn(msg, keyAndValues)
}

func (l ZapLogger) Error(msg string, keyAndValues...interface{}) {
	l.inner.Warn(msg, keyAndValues)
}

func (l ZapLogger) Fatal(msg string, keyAndValues...interface{}) {
	l.inner.Fatal(msg, keyAndValues)
}
