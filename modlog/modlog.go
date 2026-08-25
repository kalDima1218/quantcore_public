// Package modlog gives every module its own on-disk log. For(module) returns a logger
// that keeps writing to stderr exactly as before AND appends the same records to
// logs/<module>.log, so each module's errors/warnings survive in their own file.
// Trades() is the separate trade-history log (logs/trades.log): the live runners write
// one line per own fill there, so the bot's executed trades can be replayed without
// digging them out of the console noise.
//
// The directory defaults to ./logs (created on first use) and can be moved with the
// QUANTCORE_LOG_DIR environment variable. Files are opened in append mode, so restarts
// extend the history and several processes (e.g. basis-ema + basis-blend) can safely
// share trades.log — every record is a single O_APPEND write.
package modlog

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const (
	envDir     = "QUANTCORE_LOG_DIR"
	defaultDir = "logs"
	// tradesName is the module name backing Trades(); For("trades") shares the same file.
	tradesName = "trades"
	// flags stamp every record in UTC with microseconds — the resolution the trading
	// logs already use for status blocks, and unambiguous across host timezones.
	flags = log.LstdFlags | log.Lmicroseconds | log.LUTC
)

var (
	mu      sync.Mutex
	loggers = map[string]*Logger{}
)

// Level is an operational severity, carried as a typed value on Critical/Warn/Log rather
// than baked into the message text as a "CRITICAL:"/"Warning:" string prefix a downstream
// consumer (grep, an alert rule, a test) would have to parse out of free text — a prefix
// that drifts silently if a call site is worded differently or the text is edited later.
// The method name itself (Critical vs Warn) is what a call site commits to, so grepping
// for ".Critical(" finds every critical event reliably; routine logging keeps using the
// existing unleveled Printf, unchanged.
type Level int

const (
	LevelWarn Level = iota
	LevelCritical
)

func (lv Level) String() string {
	switch lv {
	case LevelCritical:
		return "CRITICAL"
	case LevelWarn:
		return "WARNING"
	default:
		return "INFO"
	}
}

// Logger wraps *log.Logger to add typed-severity logging alongside every existing call
// site's routine Printf, which keeps working unchanged via embedding.
type Logger struct {
	*log.Logger
}

// Log writes one record at level: the rendered text still carries a human-readable tag
// for operators tailing logs/<module>.log exactly as before, but level is a real
// parameter here, not something inferred from the message text afterward.
func (l *Logger) Log(level Level, format string, args ...any) {
	l.Printf("["+level.String()+"] "+format, args...)
}

// Critical logs an operationally critical event: a state the engine cannot resolve
// itself and that an operator must see (a stray fill, a naked leg, a position mismatch).
func (l *Logger) Critical(format string, args ...any) { l.Log(LevelCritical, format, args...) }

// Warn logs a degraded-but-recoverable event (a reconcile divergence, impaired mode).
func (l *Logger) Warn(format string, args ...any) { l.Log(LevelWarn, format, args...) }

func dir() string {
	if d := os.Getenv(envDir); d != "" {
		return d
	}
	return defaultDir
}

// For returns module's logger, creating logs/<module>.log on first use. Records go both
// to the file and to stderr (the pre-existing behaviour every operator setup relies on).
// If the file cannot be opened the logger degrades to stderr-only after one warning —
// a broken disk must never stop the bot from logging, let alone from trading.
func For(module string) *Logger {
	mu.Lock()
	defer mu.Unlock()
	if l, ok := loggers[module]; ok {
		return l
	}
	var w io.Writer = os.Stderr
	if testing.Testing() {
		// Unit tests exercise the same logging call sites; keep them on stderr only so
		// `go test` never litters per-package logs/ directories.
	} else if f, err := openLogFile(module); err != nil {
		log.Printf("[modlog] cannot open log file for module %q: %v — stderr only", module, err)
	} else {
		w = io.MultiWriter(os.Stderr, f)
	}
	l := &Logger{Logger: log.New(w, "", flags)}
	loggers[module] = l
	return l
}

// Trades returns the bot's trade-history logger (logs/trades.log) — one record per own
// fill, separate from every module's error log.
func Trades() *Logger {
	return For(tradesName)
}

func openLogFile(module string) (*os.File, error) {
	d := dir()
	if err := os.MkdirAll(d, 0o750); err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Join(d, module+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // module is an internal caller-chosen tag (execengine/basis/trades/...), never external input
}
