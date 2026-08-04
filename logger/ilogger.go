package logger

type ILogger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
	Debug(msg string, args ...any)
}

var _ ILogger = new(EmptyLogger)

type EmptyLogger struct {
}

func (l *EmptyLogger) Info(_ string, _ ...any)  {}
func (l *EmptyLogger) Error(_ string, _ ...any) {}
func (l *EmptyLogger) Warn(_ string, _ ...any)  {}
func (l *EmptyLogger) Debug(_ string, _ ...any) {}
