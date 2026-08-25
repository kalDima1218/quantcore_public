package recoverymachine

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 5, 8, 0, 0, 0, time.UTC)

func withCapturedLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := mlog.Writer()
	mlog.SetOutput(&buf)
	t.Cleanup(func() { mlog.SetOutput(orig) })
	return &buf
}

func TestHaltIsIdempotentAndReportsFirstTransition(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	if !m.Halt("kill switch", 5) {
		t.Fatal("the first Halt must report a transition")
	}
	if !m.Halted() {
		t.Fatal("Halted() must be true after Halt")
	}
	if m.Halt("again", 5) {
		t.Fatal("a second Halt must report no transition (idempotent)")
	}
}

func TestEnterImpairedSetsPaceAndIsIdempotent(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.EnterImpaired(t0, 2*time.Second, "reason A")
	if !m.Impaired() {
		t.Fatal("Impaired() must be true after EnterImpaired")
	}
	if want := t0.Add(2 * time.Second); !m.NextRetryAt().Equal(want) {
		t.Fatalf("NextRetryAt=%v, want %v", m.NextRetryAt(), want)
	}
	// A second call with a DIFFERENT backoff must not overwrite the pace already set —
	// idempotent, matching the original guard.
	m.EnterImpaired(t0.Add(time.Minute), 99*time.Second, "reason B")
	if want := t0.Add(2 * time.Second); !m.NextRetryAt().Equal(want) {
		t.Fatalf("a second EnterImpaired must be a no-op: NextRetryAt=%v, want %v", m.NextRetryAt(), want)
	}
}

func TestEnterImpairedNeverFiresWhileHalted(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.Halt("kill switch", 0)
	m.EnterImpaired(t0, 2*time.Second, "reason")
	if m.Impaired() {
		t.Fatal("the kill-switch must be absolute — EnterImpaired must not fire while halted")
	}
}

func TestDeferHedgeQueuesDebtAndEntersImpaired(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.DeferHedge("SI", true, 3, t0, time.Second)
	if !m.Impaired() {
		t.Fatal("DeferHedge must enter impaired mode")
	}
	debts := m.Debts()
	if len(debts) != 1 || debts[0] != (HedgeDebt{Sym: "SI", Buy: true, Lots: 3}) {
		t.Fatalf("Debts() = %+v, want one {SI true 3}", debts)
	}
}

func TestQueueRetireQueuesAndEntersImpaired(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.QueueRetire("ord1", t0, time.Second)
	if !m.Impaired() {
		t.Fatal("QueueRetire must enter impaired mode")
	}
	q := m.RetireQueue()
	if len(q) != 1 || q[0] != "ord1" {
		t.Fatalf("RetireQueue() = %v, want [ord1]", q)
	}
}

func TestSetRetireQueueAndSetDebtsReplace(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.QueueRetire("ord1", t0, time.Second)
	m.SetRetireQueue([]string{"ord2"})
	if q := m.RetireQueue(); len(q) != 1 || q[0] != "ord2" {
		t.Fatalf("RetireQueue() = %v, want [ord2]", q)
	}
	m.DeferHedge("SI", true, 1, t0, time.Second)
	m.SetDebts(nil)
	if d := m.Debts(); len(d) != 0 {
		t.Fatalf("Debts() = %v, want empty", d)
	}
}

func TestAdvancePaceResetsOnProgressAndDoublesOnStall(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.EnterImpaired(t0, 2*time.Second, "reason")

	m.AdvancePace(t0.Add(time.Second), false, 2*time.Second, 10*time.Second)
	if want := t0.Add(time.Second).Add(4 * time.Second); !m.NextRetryAt().Equal(want) {
		t.Fatalf("stall must double the gap: NextRetryAt=%v, want %v", m.NextRetryAt(), want)
	}

	m.AdvancePace(t0.Add(2*time.Second), false, 2*time.Second, 10*time.Second)
	if want := t0.Add(2 * time.Second).Add(8 * time.Second); !m.NextRetryAt().Equal(want) {
		t.Fatalf("second stall must double again: NextRetryAt=%v, want %v", m.NextRetryAt(), want)
	}

	m.AdvancePace(t0.Add(3*time.Second), false, 2*time.Second, 10*time.Second)
	if want := t0.Add(3 * time.Second).Add(10 * time.Second); !m.NextRetryAt().Equal(want) {
		t.Fatalf("gap must clamp at maxGap: NextRetryAt=%v, want %v", m.NextRetryAt(), want)
	}

	m.AdvancePace(t0.Add(4*time.Second), true, 2*time.Second, 10*time.Second)
	if want := t0.Add(4 * time.Second).Add(2 * time.Second); !m.NextRetryAt().Equal(want) {
		t.Fatalf("progress must reset the gap to baseBackoff: NextRetryAt=%v, want %v", m.NextRetryAt(), want)
	}
}

func TestClearImpairedEndsImpairedAndMarksUnverified(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.EnterImpaired(t0, 2*time.Second, "reason")
	m.ClearImpaired()
	if m.Impaired() {
		t.Fatal("ClearImpaired must end impaired mode")
	}
	if !m.Suspect() {
		t.Fatal("ClearImpaired must mark unverified — a clean reconcile pass must still confirm the position")
	}
}

func TestMarkUnverified(t *testing.T) {
	m := New("[test]")
	if m.Suspect() {
		t.Fatal("a fresh Machine must not be suspect")
	}
	m.MarkUnverified()
	if !m.Suspect() {
		t.Fatal("MarkUnverified must make Suspect() true")
	}
}

func TestDeadStreakResetAndIncrement(t *testing.T) {
	m := New("[test]")
	if got := m.IncrementDeadStreak(); got != 1 {
		t.Fatalf("first increment = %d, want 1", got)
	}
	if got := m.IncrementDeadStreak(); got != 2 {
		t.Fatalf("second increment = %d, want 2", got)
	}
	m.ResetDeadStreak()
	if got := m.TakerDeadStreak(); got != 0 {
		t.Fatalf("TakerDeadStreak after reset = %d, want 0", got)
	}
}

func TestMarkSuspectFirstPassVsSecond(t *testing.T) {
	buf := withCapturedLog(t)
	m := New("[test]")
	if !m.MarkSuspect(1, 2, 3, 4) {
		t.Fatal("the FIRST divergent pass must report a transition")
	}
	if !strings.Contains(buf.String(), "[WARNING]") {
		t.Fatalf("first divergence must log a warning, got %q", buf.String())
	}
	if m.MarkSuspect(1, 2, 3, 4) {
		t.Fatal("a SECOND consecutive divergence must report no NEW transition")
	}
}

func TestLogPersistentMismatchOnlyOncePerEpisode(t *testing.T) {
	buf := withCapturedLog(t)
	m := New("[test]")
	m.MarkSuspect(1, 2, 3, 4)
	m.LogPersistentMismatch(1, 2, 3, 4)
	if got := strings.Count(buf.String(), "POSITION MISMATCH"); got != 1 {
		t.Fatalf("first LogPersistentMismatch must log once, got %d occurrences", got)
	}
	m.LogPersistentMismatch(1, 2, 3, 4) // still suspect, still mismatched
	if got := strings.Count(buf.String(), "POSITION MISMATCH"); got != 1 {
		t.Fatalf("a repeat call within the same episode must not log again, got %d occurrences", got)
	}
	m.ResumeFromSuspect(0) // episode ends
	m.MarkSuspect(1, 2, 3, 4)
	m.LogPersistentMismatch(1, 2, 3, 4)
	if got := strings.Count(buf.String(), "POSITION MISMATCH"); got != 2 {
		t.Fatalf("a NEW episode must log again, got %d occurrences", got)
	}
}

func TestResumeFromSuspectClearsMismatchLoggedToo(t *testing.T) {
	withCapturedLog(t)
	m := New("[test]")
	m.MarkSuspect(1, 2, 3, 4)
	m.LogPersistentMismatch(1, 2, 3, 4)
	m.ResumeFromSuspect(0)
	if m.Suspect() {
		t.Fatal("ResumeFromSuspect must clear suspect")
	}
}

func TestClearUnverifiedIsANoOpWhenNotUnverified(t *testing.T) {
	buf := withCapturedLog(t)
	m := New("[test]")
	m.ClearUnverified(0)
	if buf.String() != "" {
		t.Fatalf("ClearUnverified on a never-unverified machine must log nothing, got %q", buf.String())
	}
}

func TestLogTagIsStampedOnLogLines(t *testing.T) {
	buf := withCapturedLog(t)
	m := New("[strategy_a]")
	m.Halt("kill switch", 0)
	if !strings.Contains(buf.String(), "[execengine][strategy_a]") {
		t.Fatalf("log output missing the strategy tag, got:\n%s", buf.String())
	}
}
