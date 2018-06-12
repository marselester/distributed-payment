package wallet

// Logger is the Go kit's interface.
type Logger interface {
	Log(keyvals ...interface{}) error
}

// LoggerFunc is an adapter to allow use of ordinary functions as Loggers. If
// f is a function with the appropriate signature, LoggerFunc(f) is a Logger
// object that calls f.
type LoggerFunc func(...interface{}) error

// Log implements Logger by calling f(keyvals...).
func (f LoggerFunc) Log(keyvals ...interface{}) error {
	return f(keyvals...)
}

// NoopLogger provides a logger that discards logs.
type NoopLogger struct{}

// Log implements no-op Logger.
func (l *NoopLogger) Log(_ ...interface{}) error {
	return nil
}
