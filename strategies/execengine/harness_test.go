package execengine

// Общая фикстура тестов пакета: фейковые брокерские ноги, тестовый Decider,
// конструкторы движка и билдеры RowState. Ни одного func Test — тесты живут
// в файлах, зеркалящих подсистемы: engine_clip_test.go, engine_quote_test.go,
// engine_hedge_test.go, engine_recovery_test.go, engine_orders_test.go,
// engine_sink_test.go, limiter_test.go, brokers_test.go.

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeMaker struct {
	calls         []string
	ids           map[string]string // last order id placed per symbol
	n             int
	executed      map[string]int   // test-injected lots filled before a cancel, keyed by order id
	cancelErr     map[string]error // test-injected Cancel failures, keyed by order id
	cancelErrOnce map[string]error // like cancelErr but consumed by the first Cancel (transient rpc blip)
	placeErr      map[string]error // test-injected placement failures, keyed by symbol
	// placeErrMaxLots, paired with placeErr: when set for a symbol, PlaceBid/PlaceAsk only
	// return placeErr[sym] while lots exceeds this ceiling — a broker that rejects a big
	// order but accepts a smaller one (e.g. margin-limited). Absent (the zero value, "not in
	// the map") = placeErr's plain unconditional-failure behaviour.
	placeErrMaxLots map[string]int
	attempts        []int             // every PlaceBid/PlaceAsk lots argument, in call order, success or failure — reject-retry ladder tests
	status          map[string]int    // test-injected Status executed counts (order reported terminal)
	statusLiveN     map[string]int    // test-injected: this many Status calls report the order LIVE (non-terminal, 0 executed) before the terminal `status` entry (or error) applies
	forceID         map[string]string // test-injected order id for the NEXT placement on a symbol ("" = broker returned an empty id), consumed once
}

func (m *fakeMaker) id(sym string) string { return m.ids[sym] }

// nextID picks the order id for a placement: a test-forced one (consumed once) or the
// normal generated sequence.
func (m *fakeMaker) nextID(sym, prefix string) string {
	if fid, ok := m.forceID[sym]; ok {
		delete(m.forceID, sym)
		return fid
	}
	m.n++
	return fmt.Sprintf("%s%d", prefix, m.n)
}

// rejectsAt reports whether this placement (lots) should fail, given placeErr/placeErrMaxLots.
func (m *fakeMaker) rejectsAt(sym string, lots int) bool {
	if m.placeErr[sym] == nil {
		return false
	}
	max, capped := m.placeErrMaxLots[sym]
	return !capped || lots > max
}

func (m *fakeMaker) PlaceBid(sym string, lots int, px float64) (string, error) {
	m.attempts = append(m.attempts, lots)
	if m.rejectsAt(sym, lots) {
		return "", m.placeErr[sym]
	}
	id := m.nextID(sym, "b")
	m.record(sym, id)
	m.calls = append(m.calls, fmt.Sprintf("bid %s %d @ %.2f", sym, lots, px))
	return id, nil
}

func (m *fakeMaker) PlaceAsk(sym string, lots int, px float64) (string, error) {
	m.attempts = append(m.attempts, lots)
	if m.rejectsAt(sym, lots) {
		return "", m.placeErr[sym]
	}
	id := m.nextID(sym, "a")
	m.record(sym, id)
	m.calls = append(m.calls, fmt.Sprintf("ask %s %d @ %.2f", sym, lots, px))
	return id, nil
}

func (m *fakeMaker) Cancel(id string) (int, error) {
	m.calls = append(m.calls, "cancel "+id)
	if err := m.cancelErrOnce[id]; err != nil {
		delete(m.cancelErrOnce, id)
		return 0, err
	}
	if err := m.cancelErr[id]; err != nil {
		return 0, err
	}
	return m.executed[id], nil // nil map read → 0
}

func (m *fakeMaker) Status(id string) (int, bool, error) {
	m.calls = append(m.calls, "status "+id)
	if n := m.statusLiveN[id]; n > 0 {
		m.statusLiveN[id] = n - 1
		return 0, false, nil
	}
	if n, ok := m.status[id]; ok {
		return n, true, nil
	}
	return 0, false, errors.New("status unknown")
}

func (m *fakeMaker) count(prefix string) int {
	n := 0
	for _, c := range m.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

type fakeTaker struct {
	mu       sync.Mutex
	calls    []string
	lots     map[string]int
	fail     bool
	failN    int             // test-injected: fail this many next attempts, then succeed (transient outage)
	failSym  map[string]bool // test-injected per-symbol failures (e.g. margin gone on one leg only)
	nextID   int
	lastID   string   // id returned by the most recent successful Buy/Sell
	forceIDs []string // test-injected ids for the next successful calls ("" = broker returned an empty id), consumed in order
	// failErrSym, checked BEFORE failN/fail/failSym: a SPECIFIC injected error per symbol (e.g.
	// a grpc status, so maybeDelivered classifies it), paired with failMaxLots — absent (not in
	// the map) = fail unconditionally while failErrSym[sym] is set, mirroring fakeMaker's
	// placeErr/placeErrMaxLots for the reject-retry ladder's openTakerOnlyClip tests.
	failErrSym  map[string]error
	failMaxLots map[string]int
	attempts    []string // "sym:lots" for every Buy/Sell call, success or failure, in call order
}

// issueID picks the order id for a successful cross: a test-forced one (consumed in
// order) or the normal generated sequence.
func (t *fakeTaker) issueID() string {
	if len(t.forceIDs) > 0 {
		t.lastID = t.forceIDs[0]
		t.forceIDs = t.forceIDs[1:]
		return t.lastID
	}
	t.nextID++
	t.lastID = fmt.Sprintf("t%d", t.nextID)
	return t.lastID
}

func (t *fakeTaker) add(key string, n int) {
	if t.lots == nil {
		t.lots = map[string]int{}
	}
	t.lots[key] += n
	t.calls = append(t.calls, key)
}

// rejectsAt reports the injected error for this call (sym, lots), given failErrSym/failMaxLots.
func (t *fakeTaker) rejectsAt(sym string, lots int) error {
	err := t.failErrSym[sym]
	if err == nil {
		return nil
	}
	if max, capped := t.failMaxLots[sym]; capped && lots <= max {
		return nil
	}
	return err
}

func (t *fakeTaker) Buy(sym string, lots int) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts = append(t.attempts, fmt.Sprintf("%s:%d", sym, lots))
	if err := t.rejectsAt(sym, lots); err != nil {
		return "", err
	}
	if t.failN > 0 {
		t.failN--
		return "", errors.New("taker transient")
	}
	if t.fail || t.failSym[sym] {
		return "", errors.New("taker down")
	}
	t.add("buy "+sym, lots)
	return t.issueID(), nil
}

func (t *fakeTaker) Sell(sym string, lots int) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts = append(t.attempts, fmt.Sprintf("%s:%d", sym, lots))
	if err := t.rejectsAt(sym, lots); err != nil {
		return "", err
	}
	if t.failN > 0 {
		t.failN--
		return "", errors.New("taker transient")
	}
	if t.fail || t.failSym[sym] {
		return "", errors.New("taker down")
	}
	t.add("sell "+sym, lots)
	return t.issueID(), nil
}

// barrierTaker is a Taker double that proves two calls raced the wire: each call blocks
// until BOTH have arrived, so a caller that dispatches its two legs sequentially (one
// call's return awaited before the next starts) deadlocks against it instead of
// completing — engine_clip_test.go's concurrency regression test relies on exactly that.
type barrierTaker struct {
	mu      sync.Mutex
	arrived int
	release chan struct{}
}

func newBarrierTaker() *barrierTaker {
	return &barrierTaker{release: make(chan struct{})}
}

func (t *barrierTaker) rendezvous(id string) (string, error) {
	t.mu.Lock()
	t.arrived++
	last := t.arrived == 2
	t.mu.Unlock()
	if last {
		close(t.release)
	}
	<-t.release
	return id, nil
}

func (t *barrierTaker) Buy(sym string, lots int) (string, error)  { return t.rendezvous("b-" + sym) }
func (t *barrierTaker) Sell(sym string, lots int) (string, error) { return t.rendezvous("s-" + sym) }

// testDecider is a minimal Decider double for the engine tests. It reproduces the
// open/close/position bookkeeping the spread strategy's DecisionMaker used when these
// tests lived in package spread — execengine itself must stay strategy-agnostic, so it
// cannot import spread's real decision logic (spread already imports execengine for
// Engine, and that back-import would be a cycle). The Peek/Commit branches below are the
// same shape as strategies/spread's baseDecisionMaker, just trimmed to what the engine
// itself consumes (no persistence, no trade-window predicates beyond what these tests
// exercise).
type testDecider struct {
	orderSize    int
	positionSize int
	positionMax  int
	lots         []Lot
}

// canTradeOpen/canTradeClose mirror the USD strategy's trade window (open from hour 7
// UTC, close within [7,11]). Every test in this file runs at openHour (hour == 8), so
// both are always true here, but the check is kept for behavioural fidelity with the
// original DecisionMakerUSD-backed test.
func (d *testDecider) canTradeOpen(hour int) bool  { return hour >= 7 }
func (d *testDecider) canTradeClose(hour int) bool { return hour >= 7 && hour <= 11 }

// spreadSig is the test's opaque signal payload: the six spread outputs the testDecider
// reads, carried in RowState.Signal exactly as the real spread strategy carries its own
// SignalState. It exercises the type-assert seam the engine is now neutral to.
type spreadSig struct {
	SpreadOpen            float64
	SpreadClose           float64
	SpreadOpenDiscounted  float64
	SpreadCloseDiscounted float64
	SpreadMark            float64
	SpreadTakeProfit      float64
}

func (d *testDecider) Peek(state RowState) Intent {
	hour := state.Time.Hour()
	sig, _ := state.Signal.(spreadSig)
	spreadOpen := sig.SpreadOpen
	spreadClose := sig.SpreadClose
	spreadOpenDiscounted := sig.SpreadOpenDiscounted
	spreadCloseDiscounted := sig.SpreadCloseDiscounted
	spreadMark := sig.SpreadMark
	spreadTakeProfit := sig.SpreadTakeProfit

	switch {
	case d.positionSize < d.positionMax && d.canTradeOpen(hour) && spreadOpenDiscounted > spreadMark:
		if d.positionSize < 0 {
			return Intent{Action: 1, IsClose: true}
		}
		return Intent{Action: 1, OpenPrice: spreadOpen}

	case d.positionSize > -d.positionMax && d.canTradeClose(hour) && spreadCloseDiscounted > spreadMark:
		if d.positionSize > 0 {
			return Intent{Action: -1, IsClose: true}
		}
		return Intent{Action: -1, OpenPrice: spreadClose}

	case d.positionSize < 0 && d.lots[0].Price+spreadOpen > spreadTakeProfit && spreadOpenDiscounted > 0:
		return Intent{Action: 1, IsClose: true}

	case d.positionSize > 0 && d.lots[0].Price+spreadClose > spreadTakeProfit && spreadCloseDiscounted > 0:
		return Intent{Action: -1, IsClose: true}
	}

	return Intent{Action: actionHold}
}

func (d *testDecider) Commit(in Intent, t time.Time) Decision {
	switch {
	case in.Action == actionHold:
		return Decision{Decision: actionHold}
	case in.IsClose:
		return d.closeOldest(in.Action)
	default:
		return d.openLot(in.Action, in.OpenPrice, t)
	}
}

func (d *testDecider) openLot(action int, price float64, t time.Time) Decision {
	size := action * d.orderSize
	d.positionSize += size
	d.lots = append(d.lots, Lot{Price: price, Size: size, Time: t})
	slices.SortFunc(d.lots, func(a, b Lot) int {
		return cmp.Compare(a.Price, b.Price)
	})
	return Decision{Decision: action}
}

func (d *testDecider) closeOldest(action int) Decision {
	d.positionSize += action * d.orderSize
	closed := d.lots[0]
	d.lots = d.lots[1:]
	return Decision{Decision: action, ClosedPosition: &closed}
}

func (d *testDecider) Position() int { return d.positionSize }
func (d *testDecider) SaveLots()     {} // no persistence in tests

const (
	testLegA = "SI"
	testLegB = "UF"
)

// openHour is a UTC time inside the USD open window (hour ≥ 7) so the testDecider's
// canTradeOpen/canTradeClose predicates permit a move.
var openHour = time.Date(2026, 1, 5, 8, 0, 0, 0, time.UTC)

func newTestEngine(m Maker, tk Taker, orderVol int) *Engine {
	dm := newTestDecider(20, orderVol) // no persistence
	return newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: orderVol,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
	}, m, tk, dm)
}

// newEngineWith is the tests' config-form constructor (a plain NewEngine alias, kept as
// the single seam should tests ever need engine tweaks again).
func newEngineWith(cfg EngineConfig, m Maker, tk Taker, dm Decider) *Engine {
	return NewEngine(cfg, m, tk, dm)
}

// seedBooks feeds a valid, fresh two-leg book to the engine.
func seedBooks(e *Engine, ts time.Time) {
	e.OnBook(testLegA, ts, 100, 101)
	e.OnBook(testLegB, ts, 50, 51)
}

// buyState is a signal bar that makes the Decider want to open a long position.
func buyState(ts time.Time) RowState {
	return RowState{Time: ts, Signal: spreadSig{SpreadOpen: 5, SpreadOpenDiscounted: 100, SpreadMark: 0}}
}

// sellState is a signal bar that makes the Decider want to open a short position.
func sellState(ts time.Time) RowState {
	return RowState{Time: ts, Signal: spreadSig{SpreadClose: 5, SpreadCloseDiscounted: 100, SpreadMark: 0}}
}

// holdState is a signal bar that makes the Decider want nothing (all spreads at the
// mark), so a resting clip is no longer wanted.
func holdState(ts time.Time) RowState {
	return RowState{Time: ts, Signal: spreadSig{}}
}

// newSoloTestEngine builds a single-passive engine: LegA (the dated leg) is the ONLY maker
// leg; LegB (the perp leg) is never rested and is always taker-hedged on the LegA fill.
func newSoloTestEngine(m Maker, tk Taker, orderVol int) *Engine {
	dm := newTestDecider(20, orderVol)
	return newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: orderVol,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		SoloMakerLeg: true,
	}, m, tk, dm)
}

// newSoloRatioTestEngine builds a single-passive engine whose LegB hedge is `ratio` contracts
// per ONE LegA contract — the asymmetric-notional pair (one index future against ten minis).
// Solo is the only mode HedgeRatio supports; see EngineConfig.HedgeRatio.
func newSoloRatioTestEngine(m Maker, tk Taker, orderVol, ratio int) *Engine {
	dm := newTestDecider(20, orderVol)
	return newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: orderVol,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		SoloMakerLeg: true, HedgeRatio: ratio,
	}, m, tk, dm)
}

// newTakerOnlyTestEngine builds an engine where a decided move never rests a maker order —
// both legs cross the book as takers immediately.
func newTakerOnlyTestEngine(m Maker, tk Taker, orderVol int) *Engine {
	dm := newTestDecider(20, orderVol)
	return newEngineWith(EngineConfig{
		LegA: testLegA, LegB: testLegB, OrderVol: orderVol,
		FillTimeout: 2 * time.Minute, HedgeRetries: 2,
		TakerOnly: true,
	}, m, tk, dm)
}

// fixedDecider is a Decider double that always emits a preset intent — used to exercise the
// engine's self-sizing (Intent.Lots) and close-guarantee (ForceCloseOnTimeout) generalizations
// independently of any strategy's decision logic.
type fixedDecider struct {
	intent Intent
	pos    int
}

func (d *fixedDecider) Peek(RowState) Intent { return d.intent }
func (d *fixedDecider) Commit(in Intent, _ time.Time) Decision {
	if in.Action == actionHold {
		return Decision{}
	}
	d.pos += in.Action * in.Lots
	return Decision{Decision: in.Action}
}
func (d *fixedDecider) Position() int { return d.pos }

func newTestDecider(positionMax, orderSize int) *testDecider {
	return &testDecider{orderSize: orderSize, positionMax: positionMax}
}

// recordSink captures FillSink credits so tests can assert the position the live ledger
// (and thus the Decider's cap) would see at each moment.
type recordSink struct {
	events     []string
	netA, netB int
}

func (s *recordSink) Fill(sym string, buy bool, lots int, price float64) {
	s.events = append(s.events, fmt.Sprintf("fill %s buy=%v x%d @%.2f", sym, buy, lots, price))
	d := lots
	if !buy {
		d = -lots
	}
	if sym == testLegA {
		s.netA += d
	} else {
		s.netB += d
	}
}

func (s *recordSink) Amend(sym string, buy bool, lots int, from, to float64) {
	s.events = append(s.events, fmt.Sprintf("amend %s buy=%v x%d %.2f->%.2f", sym, buy, lots, from, to))
}

// stubLimiter is a hand-driven Limiter double: tests flip ok/retryAt between calls and
// read spent to assert how many placement RPCs the engine booked against the budget.
type stubLimiter struct {
	ok      bool
	retryAt time.Time
	spent   int
	allowN  int // if > 0, Allow returns ok only for the first allowN calls, then false (0 = uncapped, ok governs every call)
	calls   int
}

func (s *stubLimiter) Allow(time.Time, int) (bool, time.Time) {
	s.calls++
	if s.allowN > 0 && s.calls > s.allowN {
		return false, s.retryAt
	}
	return s.ok, s.retryAt
}
func (s *stubLimiter) Spend(_ time.Time, ops int) { s.spent += ops }

func (m *fakeMaker) record(sym, id string) {
	if m.ids == nil {
		m.ids = map[string]string{}
	}
	m.ids[sym] = id
}

// completeBuyClip opens a 2-lot buy clip, fills the maker leg via the stream and lets the
// engine hedge legB with a taker, committing the clip. Returns the taker's order id.
func completeBuyClip(t *testing.T, e *Engine, m *fakeMaker, tk *fakeTaker) string {
	t.Helper()
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)
	if bidA == "" {
		t.Fatal("clip did not open")
	}
	e.OnFill(openHour.Add(time.Second), bidA, testLegA, true, 2, 100)
	if e.Working() {
		t.Fatal("clip must commit once the maker leg is fully filled")
	}
	if tk.lastID == "" {
		t.Fatal("the maker fill must have been hedged with a taker")
	}
	return tk.lastID
}

// completeBuyClipNoID is completeBuyClip for the empty-taker-id case (tk.lastID stays "").
func completeBuyClipNoID(t *testing.T, e *Engine, m *fakeMaker) {
	t.Helper()
	seedBooks(e, openHour)
	e.OnState(buyState(openHour))
	bidA := m.id(testLegA)
	if bidA == "" {
		t.Fatal("clip did not open")
	}
	e.OnFill(openHour.Add(time.Second), bidA, testLegA, true, 2, 100)
	if e.Working() {
		t.Fatal("clip must commit once the maker leg is fully filled")
	}
}
