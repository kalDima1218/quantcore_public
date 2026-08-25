package modlog

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// newTestLogger builds a Logger writing to a buffer, bypassing For's file/stderr
// plumbing so the leveled-logging behavior can be asserted directly.
func newTestLogger(buf *bytes.Buffer) *Logger {
	return &Logger{Logger: log.New(buf, "", 0)}
}

func TestLoggerCriticalCarriesLevelAsData(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	l.Critical("stray fill of %d lots on %s", 3, "SiU6")

	got := buf.String()
	if !strings.Contains(got, "[CRITICAL]") {
		t.Fatalf("output %q does not carry a CRITICAL level tag", got)
	}
	if !strings.Contains(got, "stray fill of 3 lots on SiU6") {
		t.Fatalf("output %q does not contain the formatted message", got)
	}
}

func TestLoggerWarnCarriesLevelAsData(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	l.Warn("reconcile diverged: legA have=%d want=%d", 5, 6)

	got := buf.String()
	if !strings.Contains(got, "[WARNING]") {
		t.Fatalf("output %q does not carry a WARNING level tag", got)
	}
}

// TestLoggerLogDispatchesOnTypedLevel proves severity is a real parameter a caller can
// switch on — not something that only exists once baked into rendered text — by driving
// both levels through the single Log(level, ...) entry point Critical/Warn wrap.
func TestLoggerLogDispatchesOnTypedLevel(t *testing.T) {
	cases := []struct {
		level Level
		want  string
	}{
		{LevelCritical, "[CRITICAL]"},
		{LevelWarn, "[WARNING]"},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		l := newTestLogger(&buf)
		l.Log(c.level, "event")
		if !strings.Contains(buf.String(), c.want) {
			t.Errorf("Log(%v, ...) = %q, want it to contain %q", c.level, buf.String(), c.want)
		}
	}
}

// TestLoggerPrintfStillWorks pins that the embedding preserves every existing call
// site's routine, unleveled logf(...)/Printf(...) usage across the codebase unchanged.
func TestLoggerPrintfStillWorks(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	l.Printf("routine: %s", "ok")

	if got := buf.String(); !strings.Contains(got, "routine: ok") {
		t.Fatalf("Printf output = %q, want it to contain the formatted message", got)
	}
}
