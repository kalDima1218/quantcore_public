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
	loggers = map[string]*log.Logger{}
)

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
func For(module string) *log.Logger {
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
	l := log.New(w, "", flags)
	loggers[module] = l
	return l
}

// Trades returns the bot's trade-history logger (logs/trades.log) — one record per own
// fill, separate from every module's error log.
func Trades() *log.Logger {
	return For(tradesName)
}

func openLogFile(module string) (*os.File, error) {
	d := dir()
	if err := os.MkdirAll(d, 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Join(d, module+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}
