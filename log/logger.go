package log

import (
	"go.uber.org/zap"
	"net/http"
	"time"
)

type Logger interface {
	Debug(msg string, keyAndValues ...interface{})
	Info(msg string, keyAndValues ...interface{})
	Warn(msg string, keyAndValues ...interface{})
	Error(msg string, keyAndValues ...interface{})
	Fatal(msg string, keyAndValues ...interface{})
}

type statusRecorder struct {
	http.ResponseWriter
	Status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.Status = code
	rec.ResponseWriter.WriteHeader(code)
}

type LoggingHandler struct {
	handler http.Handler
	logger  Logger
}

func NewLoggingHandler(handler http.Handler, logger Logger) *LoggingHandler {
	return &LoggingHandler{
		handler,
		logger,
	}
}

func (h *LoggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	statusRec := &statusRecorder{ResponseWriter: w, Status: http.StatusOK}

	h.handler.ServeHTTP(statusRec, r)

	if statusRec.Status < 300 {
		h.logger.Info("processed request",
			"requestURI", r.RequestURI,
			"method", r.Method,
			"status", statusRec.Status,
			"elapsed", time.Since(start))
	} else {
		h.logger.Error("error processing request",
			"requestURI", r.RequestURI,
			"method", r.Method,
			"status", statusRec.Status,
			"elapsed", time.Since(start))
	}
}

type ZapLogger struct {
	inner *zap.SugaredLogger
}

func NewZapLogger(log *zap.Logger) ZapLogger {
	return ZapLogger{inner: log.Sugar()}
}

func (l ZapLogger) Debug(msg string, keyAndValues ...interface{}) {
	l.inner.Debugw(msg, keyAndValues...)
}

func (l ZapLogger) Info(msg string, keyAndValues ...interface{}) {
	l.inner.Infow(msg, keyAndValues...)
}

func (l ZapLogger) Warn(msg string, keyAndValues ...interface{}) {
	l.inner.Warnw(msg, keyAndValues...)
}

func (l ZapLogger) Error(msg string, keyAndValues ...interface{}) {
	l.inner.Errorw(msg, keyAndValues...)
}

func (l ZapLogger) Fatal(msg string, keyAndValues ...interface{}) {
	l.inner.Fatalw(msg, keyAndValues...)
}
