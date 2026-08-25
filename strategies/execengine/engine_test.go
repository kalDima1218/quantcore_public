package execengine

// Тесты ядра движка: приём филлов через OnFill, поведение остановленного
// движка и рандомизированные штормы событий, проверяющие, что ноги остаются
// спаренными. Зеркалит engine.go. Подсистемы — в соседних файлах:
// engine_clip_test.go, engine_quote_test.go, engine_hedge_test.go,
// engine_recovery_test.go, engine_orders_test.go, engine_sink_test.go.
// Фикстура — в harness_test.go.

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestCriticalAndWarnCarryTypedSeverity pins that e.critical/e.warn route through
// modlog's typed Level methods (see modlog.Logger.Critical/Warn) rather than baking a
// "CRITICAL:"/"Warning:" string into the format text by hand at each of the ~25 call
// sites that need it — the level is a real parameter on the shared mlog logger, not a
// convention every caller has to remember to spell the same way.
func TestCriticalAndWarnCarryTypedSeverity(t *testing.T) {
	var buf bytes.Buffer
	mlog.SetOutput(&buf)
	defer mlog.SetOutput(os.Stderr) // modlog.For writes stderr-only under go test

	e := newTestEngine(&fakeMaker{}, &fakeTaker{}, 2)
	e.critical("stray fill of %d lots on %s", 3, testLegA)
	e.warn("reconcile diverged: have=%d want=%d", 5, 6)

	got := buf.String()
	if !strings.Contains(got, "[CRITICAL]") || !strings.Contains(got, "stray fill of 3 lots") {
		t.Fatalf("critical output = %q, want a [CRITICAL] tag and the formatted message", got)
	}
	if !strings.Contains(got, "[WARNING]") || !strings.Contains(got, "reconcile diverged: have=5 want=6") {
		t.Fatalf("warn output = %q, want a [WARNING] tag and the formatted message", got)
	}
}

// Zero and negative lot counts are malformed stream noise: they must not designate a
// maker, move accounts, or place orders.
func TestZeroAndNegativeLotFillsAreNoOps(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)

	e.OnFill(openHour, bidA, testLegA, true, 0, 100)
	e.OnFill(openHour, bidA, testLegA, true, -3, 100)

	if e.clip.makerID != "" {
		t.Fatal("an empty fill must not designate the maker")
	}
	if len(tk.calls) != 0 || len(m.calls) > 2 {
		t.Fatalf("no orders may move on empty fills, taker=%v maker=%v", tk.calls, m.calls)
	}
	if acct := e.own[bidA]; acct.Seen != 0 || acct.Folded != 0 {
		t.Fatalf("accounts must not move on empty fills, got %+v", acct)
	}
}

// A halted engine fed the full event storm (books, bars, ticks, statuses, fills on unknown
// ids) must never place an order — the kill-switch is absolute across every entry point.
func TestHaltedEngineSurvivesEventStormWithoutTrading(t *testing.T) {
	m, tk := &fakeMaker{}, &fakeTaker{}
	e := newTestEngine(m, tk, 2)
	seedBooks(e, openHour)
	e.Halt("test")
	placements := m.n

	for i := 0; i < 50; i++ {
		ts := openHour.Add(time.Duration(i) * time.Second)
		e.OnBook(testLegA, ts, 100+float64(i%3), 101+float64(i%3))
		e.OnBook(testLegB, ts, 50, 51)
		e.OnState(buyState(ts))
		e.OnState(sellState(ts))
		e.OnTick(ts)
		e.OnOrderStatus(fmt.Sprintf("ghost%d", i), true)
		e.OnFill(ts, fmt.Sprintf("ghost%d", i), testLegA, true, 1, 100)
		e.Reconcile(7, -3)
	}

	if m.n != placements || len(tk.calls) != 0 {
		t.Fatalf("a halted engine may never trade, maker placements %d→%d taker=%v", placements, m.n, tk.calls)
	}
}

// The engine's core promise is that every own execution it acts on is matched on the other
// leg within the same handler call: after ANY entry point returns (and absent injected
// broker failures), the sink's two legs are exact mirrors. This property test drives a few
// hundred randomized steps — book moves (re-pegs), signal bars (opens/reverts/reversals),
// honest partial fills, cancel-catches armed at random, timeout ticks and venue kills — with
// an honest fake broker (acks always ≥ streamed lots, fills never exceed placed sizes) and
// asserts the pairing invariant plus no-halt after every single step. Fixed seeds keep every
// run reproducible.
func TestRandomizedEventStormKeepsLegsPaired(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			m := &fakeMaker{executed: map[string]int{}}
			tk := &fakeTaker{}
			e := newTestEngine(m, tk, 1+rng.Intn(4))
			sink := &recordSink{}
			e.SetFillSink(sink)
			ts := openHour
			bidA, bidB := 100.0, 50.0
			e.OnBook(testLegA, ts, bidA, bidA+1)
			e.OnBook(testLegB, ts, bidB, bidB+1)

			// syncAcks keeps the fake broker honest: a cancel-ack can never report fewer lots
			// than its fill stream already delivered (the downward contradiction has its own
			// dedicated test above and would legitimately unpair the book).
			syncAcks := func() {
				for id, acct := range e.own {
					if acct.Maker && acct.Final < 0 && m.executed[id] < acct.Seen {
						m.executed[id] = acct.Seen
					}
				}
			}
			// liveMakers lists own maker orders that can still receive fill events, in a
			// deterministic order (map iteration would break seed reproducibility).
			liveMakers := func() []string {
				var ids []string
				for id, acct := range e.own {
					if !acct.Maker {
						continue
					}
					limit := acct.Placed
					if acct.Final >= 0 {
						limit = acct.Final
					}
					if acct.Seen < limit {
						ids = append(ids, id)
					}
				}
				sort.Strings(ids)
				return ids
			}

			for step := 0; step < 400; step++ {
				ts = ts.Add(time.Duration(1+rng.Intn(2000)) * time.Millisecond)
				syncAcks()
				switch rng.Intn(10) {
				case 0, 1: // a leg's touch moves — may out-quote a resting order and re-peg it
					if rng.Intn(2) == 0 {
						bidA += float64(rng.Intn(3)-1) * 0.5
						e.OnBook(testLegA, ts, bidA, bidA+1)
					} else {
						bidB += float64(rng.Intn(3)-1) * 0.5
						e.OnBook(testLegB, ts, bidB, bidB+1)
					}
				case 2, 3, 4: // a signal bar: open, keep, revert or reverse
					switch rng.Intn(3) {
					case 0:
						e.OnState(buyState(ts))
					case 1:
						e.OnState(sellState(ts))
					default:
						e.OnState(holdState(ts))
					}
				case 5, 6: // an honest fill event on a random own maker order
					ids := liveMakers()
					if len(ids) == 0 {
						continue
					}
					id := ids[rng.Intn(len(ids))]
					acct := e.own[id]
					limit := acct.Placed
					if acct.Final >= 0 {
						limit = acct.Final
					}
					lots := 1 + rng.Intn(limit-acct.Seen)
					if acct.Final < 0 && m.executed[id] < acct.Seen+lots {
						m.executed[id] = acct.Seen + lots // the ack keeps up with the stream
					}
					e.OnFill(ts, id, acct.Sym, acct.IsBuy, lots, acct.Price)
				case 7: // arm a cancel-catch: resting lots that will fill during a future cancel
					ids := liveMakers()
					if len(ids) == 0 {
						continue
					}
					id := ids[rng.Intn(len(ids))]
					acct := e.own[id]
					if acct.Final < 0 && m.executed[id] < acct.Placed {
						m.executed[id] += 1 + rng.Intn(acct.Placed-m.executed[id])
					}
				case 8: // the clock advances — the fill-timeout backstop may fire
					e.OnTick(ts)
				case 9: // the venue kills a random resting order (its ack stays honest)
					ids := liveMakers()
					if len(ids) == 0 {
						continue
					}
					id := ids[rng.Intn(len(ids))]
					if acct := e.own[id]; acct.Final < 0 {
						e.OnOrderStatus(id, true)
					}
				}
				if e.Halted() {
					t.Fatalf("step %d: an honest broker must never halt the engine", step)
				}
				if sink.netA != -sink.netB {
					t.Fatalf("step %d: legs unpaired — netA=%d netB=%d", step, sink.netA, sink.netB)
				}
			}
			e.CancelClip() // shutdown: the final teardown must leave the book paired too
			if sink.netA != -sink.netB {
				t.Fatalf("legs unpaired after shutdown — netA=%d netB=%d", sink.netA, sink.netB)
			}
		})
	}
}
