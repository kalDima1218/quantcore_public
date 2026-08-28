package execengine2

import "QuantCore/modlog"

type logWriter struct {
	backend *modlog.Logger
	prefix  string
}

func newLogger(logTag string) *logWriter {
	return &logWriter{backend: modlog.For("execengine2"), prefix: "[execengine2]" + logTag + " "}
}

func (l *logWriter) Infof(format string, args ...any) {
	l.backend.Printf(l.prefix+format, args...)
}

func (l *logWriter) Warnf(format string, args ...any) {
	l.backend.Warn(l.prefix+format, args...)
}

func (l *logWriter) Criticalf(format string, args ...any) {
	l.backend.Critical(l.prefix+format, args...)
}
