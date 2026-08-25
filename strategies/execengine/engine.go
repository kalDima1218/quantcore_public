// Engine core: the config and state shared by every path, the constructor, and OnFill.
//
// OnFill deliberately stays here rather than moving to the clip or hedge file: it is the
// one place where the registry (dedup a re-delivered event), the clip (designate the
// maker, resolve the pair 1:1) and hedging (cover a stray or excess fill) must agree
// within a single event. Splitting it would scatter one invariant across three files.
//
// The rest of the engine lives in engine_{clip,quote,orders,hedge,recovery}.go — same
// type, same single-goroutine contract, grouped by which state they own.

package execengine

import (
	"time"

	"QuantCore/modlog"
)

// mlog is the execengine module's log: everything below still reaches stderr, and is
// also appended to logs/execengine.log.
var mlog = modlog.For("execengine")

// EngineConfig parameterizes the dual-maker execution layer shared by the dual-leg
// strategies. The trading signal (which direction to move and when) is owned by the
// Decider; this config only governs how a decided move is worked into the two order
// books.
type EngineConfig struct {
	LegA              string        // first leg's symbol
	LegB              string        // second leg's symbol
	OrderVol          int           // contracts per clip (one Decider lot)
	FillTimeout       time.Duration // OPTIONAL backstop: pull/complete a clip after this long unfilled; 0 = wait for the signal (recommended)
	HedgeRetries      int           // taker-hedge attempts before halting on a naked leg (min 1)
	MaxStaleness      time.Duration // refuse to quote if a leg's book is older than this; 0 disables
	ReconcileInterval time.Duration // runner: gap between broker-position reconciliations; 0 → default
	PlaceRetryBackoff time.Duration // pause before opening a new clip after a placement failure / rate-limit denial; 0 → 2s default
	// RejectRetryLotStep / RejectRetryMinLots implement a shrink-and-retry ladder, in two
	// places that share the same knobs but not the same reasoning:
	//
	// 1. A CLOSING clip's (Intent.IsClose) FIRST placement attempt (tryOpenClip/openClip):
	// when the broker DEFINITIVELY rejects it (MaybeDelivered == false — the broker
	// answered, nothing rests; e.g. insufficient margin, not a transport hiccup or the
	// ambiguous/corrupted-id cases placeLeg itself flags), the engine retries the SAME clip
	// at lots-RejectRetryLotStep, then again, down to RejectRetryMinLots (floor 1 if
	// unset), all within one tryOpenClip call — each rung re-checks the rate limiter so a
	// reject storm cannot silently spend past the shared placeOrder budget. Deliberately
	// NOT applied to an OPENING clip (!IsClose): its size is the Decider's entry sizing
	// (room under the cap/SizeGate), and shrinking it on a reject would silently under-open
	// a position the signal asked for a specific size of — a rejected exit has no such
	// ambiguity, getting some of it off beats none. Within closes it still only applies to
	// a clip's FIRST leg placement (dual/solo-maker's opening placeLeg, before anything
	// else has been placed): it does NOT retry dual-passive's legB-after-legA-placed unwind
	// (legA may already have a race fill; shrinking there could overshoot the close past
	// flat into a reverse). Once ANY size opens, the shortfall from the original
	// Intent.Lots needs no separate top-up: ledger position reflects only real fills, and
	// the Decider's next Peek recomputes abs(pos) from it, asking for the remainder itself.
	//
	// 2. ANY taker hedge (takerRetryFrom, shared by openTakerOnlyClip's per-leg catch-up,
	// dual/solo-maker's post-fill cross, and every shortfall re-hedge): a hedge is an
	// amount already OWED — a fill, or a sibling leg's own placement, already moved — not
	// a size being decided, so unlike (1) it applies REGARDLESS of Intent.IsClose. Nothing
	// requires the owed amount to arrive as one order: on a definitive reject it shrinks
	// and keeps chasing the remainder, accumulating partial placements toward the same
	// total, down to the same floor. Only what the floor still could not place falls
	// through to the unchanged HedgeRetries attempts loop and, on exhaustion, deferHedge.
	//
	// 0 (default) disables both: one attempt, then the unchanged PlaceRetryBackoff /
	// HedgeRetries behaviour.
	RejectRetryLotStep int
	RejectRetryMinLots int
	RepegThrottle      time.Duration // min gap between re-pegs of the SAME leg; bounds a fast-moving leg's re-peg rate; 0 → 500ms default
	MinRest            time.Duration // guaranteed resting time: a just-placed (or just re-pegged) passive order is neither
	//                                 re-pegged again nor pulled by PullIfUnwanted until it has rested this long at its price.
	//                                 Every cancel/replace burns request quota and forfeits queue priority, and a passive
	//                                 order needs time in the book to be traded against — without a floor, a z flickering
	//                                 around its gate churns place→pull within milliseconds. 0 = no guarantee (unchanged:
	//                                 re-pegs spaced only by RepegThrottle, pulls immediate). Safety teardowns — halt,
	//                                 impaired, stale-book pull, fill-timeout, shutdown — ignore it: they never wait on a
	//                                 courtesy timer.
	LogTag string // per-instance tag appended to "[execengine]" in every log line (e.g. "[basis_ema]"), so
	//                                 two strategies sharing a process stay distinguishable in execengine.log; "" → untagged
	DisableRepeg bool //               never re-peg a resting clip's passive orders to a moved touch (post-and-wait). No
	//                                 production strategy sets it anymore: spread always followed the touch, and basis
	//                                 dropped post-and-wait too (a resting order left off-touch fills only adversely, so
	//                                 its runner/backtest pull a decayed entry and let OnBook re-peg the rest). Kept as an
	//                                 opt-out for experiments and tests.
	SoloMakerLeg bool //               single-passive execution: post ONLY LegA as a maker and always taker-hedge LegB on the
	//                                 LegA fill, instead of resting both legs (dual-passive) and taking whichever loses the
	//                                 fill race. Solo is the degenerate dual case where LegB always loses: openClip skips the
	//                                 LegB placement and the existing first-fill path (retire the empty counterpart → 0 →
	//                                 taker the full leg) hedges it. Default false = unchanged dual-passive behaviour.
	HedgeRatio int // contracts of LegB that hedge ONE contract of LegA, for pairs whose legs carry
	//                                 different notional per contract (e.g. one index future against ten
	//                                 mini-futures on the same index). 0 or 1 = the 1:1 pairing every
	//                                 existing strategy trades, byte-for-byte unchanged.
	//
	//                                 REQUIRES SoloMakerLeg or TakerOnly — the two modes where LegB never
	//                                 rests as a passive, so the conversion only ever runs ONE way. Under
	//                                 solo, LegA is the sole passive and a LegA fill of n contracts is
	//                                 taker-hedged with n×R; under taker-only both sizes are known before
	//                                 either order is sent. Neither divides. Dual-passive would have to
	//                                 convert the other way too — 7 LegB contracts against R=10 is not a
	//                                 whole LegA lot — and the remainder would need somewhere to live: a
	//                                 new class of engine state in exactly the paths that have historically
	//                                 produced races and phantom hedges. A ratio in dual-passive is
	//                                 therefore a MISCONFIGURATION: the engine halts on construction rather
	//                                 than quietly hedging 1:1, which would be a silent R-fold under-hedge
	//                                 (see NewEngine).
	//
	//                                 Everything the engine counts stays in LegA contracts — the Decider's
	//                                 position and cap, clip targets, makerFilled, settle's `want` — and the
	//                                 ratio is applied ONLY where a LegB order is actually sized. Reconcile
	//                                 is the one accounting spot that must know: the broker reports LegB in
	//                                 its own contracts, so a position of P means −P×R there.
	TakerOnly bool //                 no maker legs at all: both legs cross the book as market orders
	//                                 the instant a clip opens, instead of resting either passively.
	//                                 openClip skips BOTH placeLeg calls and crosses LegA then LegB via
	//                                 the existing taker path (takerRetry), then commits the Decider's
	//                                 intent immediately — there is no resting order for OnFill to
	//                                 designate as the maker, so the clip never becomes "working".
	//                                 Mutually exclusive with SoloMakerLeg (validated by the caller,
	//                                 e.g. basis.Config.Validate). Default false = unchanged behaviour.
	ForceCloseOnTimeout bool // on a fill-timeout, a CLOSING clip (Intent.IsClose) is finished with a taker cross even if
	//                                 nothing filled passively — a reduction, once decided, is guaranteed. Opening clips are still
	//                                 abandoned (cancelled) on timeout. Spread leaves this false (its clips complete only once a
	//                                 leg has begun filling); basis sets it so a reverted position is always taken off. Note the
	//                                 guarantee applies to a STILL-WANTED close: basis pulls the unfilled remainder of an exit
	//                                 whose closing z has sunk back below −ExitZ (see PullIfUnwanted) before the timeout can
	//                                 force-cross it at a worse-than-fair price.
	KeepPartialOpenOnTimeout bool // on a fill-timeout, an OPENING clip (!Intent.IsClose) that has PARTIALLY filled is
	//                                 cancelled rather than taker-completed: the already-hedged partial fill is kept (it is
	//                                 already reflected in position — fills are booked as they happen) and the unfilled
	//                                 remainder is simply dropped, with no taker chase. Spread leaves this false: its clips
	//                                 taker-complete once any leg has begun filling, matching its committed lot book. Basis
	//                                 sets it true to reproduce the old basis Engine's checkTimeout, which cancelled a
	//                                 partially-filled ENTRY instead of crossing the spread to finish it.
	TakerConfirmTimeout time.Duration // how long a placed taker may go without its fill events before the engine treats the
	//                                 hedge as UNCONFIRMED: new clips stop opening and the order's status is polled until the
	//                                 broker confirms it terminal (fully filled → resume; short → un-credit and re-hedge the
	//                                 shortfall). 0 → 10s default. See checkPendingTakers.
	PullOnStaleBook bool //            pull the working clip's resting orders when a leg's book exceeds MaxStaleness (a dead
	//                                 market-data feed means resting quotes are priced blind) and let trading resume by itself
	//                                 once fresh books arrive. Belongs in the SHARED strategy config: a backtest that quotes
	//                                 through gaps the live bot sits out reports a different trade structure than the bot it
	//                                 is supposed to model. It used to be runner-only, on the theory that a feed's quiet gaps
	//                                 are legitimate rather than an outage — but the pull fires on both alike, and measuring
	//                                 the difference (2026-07-26) settled it: over 20 trading days the gate is worth −1.3% of
	//                                 net (t=−0.83, i.e. nothing), while on a thin Sunday session it moved the backtest's
	//                                 passive fill rate from 30% to 10% against the live bot's 9%.
}

// Engine drives the dual-passive execution state machine shared by the dual-leg
// strategies. It is fed market, fill and time events and issues maker/taker order
// operations; it owns no goroutines so it can be unit-tested by calling its handlers
// directly. The strategy's direction is supplied per bar by the Decider (via OnState):
// on a decided move place two passive orders at the touch, and when one fills, take the
// other leg to complete the pair.
//
// CONCURRENCY CONTRACT: the engine holds no locks — every method (handlers, Reconcile,
// CancelClip, Halt, accessors) must be called from ONE goroutine, the runner's event
// loop. Anything crossing goroutines lives behind its own synchronization (QuotaLimiter's
// mutex, the runners' channels); handing the engine itself to a second goroutine is a
// data race by definition.
type Engine struct {
	cfg     EngineConfig
	dm      Decider
	maker   Maker
	taker   Taker
	limiter Limiter
	clock   Clock    // processing-time source for quota bookkeeping (Allow/Spend) — see clock.go
	sink    FillSink // optional (live only): fed the engine's acted-on executions as they happen — see FillSink; credited in OnFill/finishRetire/tryPlaceTaker

	backoffUntil time.Time // suppress opening new clips until this time (rate-limit / failure backoff)

	legA touch
	legB touch
	clip *clip // the in-flight clip (nil when idle), at most one at a time

	// recovery groups the kill-switch, connection-trouble mode, reconcile-divergence
	// tracking and the deferred-obligation queues — see recovery.go for why these fields
	// are grouped but their transition methods stay on Engine.
	recovery recovery

	// now is the engine's data-driven clock: the latest event time any handler has seen
	// (monotonic max — book timestamps can lag tick times). It exists so state that needs a
	// timestamp outside a handler that carries one (a taker's placement time) never reads the
	// wall clock, which would make backtests and unit tests non-deterministic.
	now time.Time

	// pending — see hedge.go for what it owns and why its type is named but its
	// transitions (checkPendingTakers, placeTakerRPC's caller) stay on Engine.
	pending pendingSet

	// own maps every order id this engine has placed THIS process to its fill account —
	// see ledger.go for what it owns and why its type is named but its transitions
	// (OnFill, OnOrderStatus, retireOrder) stay on Engine.
	own ledger
}

// NewEngine builds an execution engine for cfg driven by maker/taker order operations
// and dm's decisions.
func NewEngine(cfg EngineConfig, maker Maker, taker Taker, dm Decider) *Engine {
	e := &Engine{
		cfg: cfg, dm: dm, maker: maker, taker: taker, limiter: noLimit{}, clock: realClock{},
		own:     ledger{},
		pending: pendingSet{},
	}
	// A hedge ratio outside single-passive mode is a misconfiguration the engine must not
	// trade through. It cannot convert a LegB fill back into whole LegA lots, and the only
	// alternative to refusing — pairing 1:1 anyway — is a silent R-fold under-hedge: the
	// book looks balanced in every internal count while carrying R times the intended
	// directional exposure. A frozen engine is strictly safer, so it starts halted and says
	// why. (NewEngine returns no error by design — the strategy configs validate first; this
	// is the backstop for a caller that builds an EngineConfig directly.)
	if cfg.HedgeRatio > 1 && !cfg.SoloMakerLeg && !cfg.TakerOnly {
		e.recovery.halted = true
		e.logf("HALTED at construction: HedgeRatio=%d requires SoloMakerLeg or TakerOnly (LegB must never rest as a passive, or a LegB fill could not be converted to whole LegA lots) — refusing to trade rather than hedging 1:1, which would be a %d-fold under-hedge", cfg.HedgeRatio, cfg.HedgeRatio)
	}
	return e
}

// hedgeDebt is one taker hedge the engine owes the book but has not managed to place.
type hedgeDebt struct {
	sym  string
	buy  bool
	lots int
}

// advanceNow moves the engine's data-driven clock forward to ts (never backward — book
// timestamps can trail tick times). Every handler that carries a timestamp folds it in.
func (e *Engine) advanceNow(ts time.Time) {
	if ts.After(e.now) {
		e.now = ts
	}
}

// SetLimiter installs a rate limiter consulted before opening clips (live only). Call
// once before the engine starts processing events; a nil limiter is ignored.
func (e *Engine) SetLimiter(l Limiter) {
	if l != nil {
		e.limiter = l
	}
}

// SetClock installs the processing-time source quota bookkeeping (Allow/Spend) reads —
// live callers never need this (realClock is the default); tests and Backtest inject a
// controllable Clock to keep quota-window assertions deterministic. Call once before the
// engine starts processing events; a nil clock is ignored.
func (e *Engine) SetClock(c Clock) {
	if c != nil {
		e.clock = c
	}
}

// SetFillSink installs the sink fed the engine's own-execution accounting (see FillSink).
// LIVE ONLY: backtests must not set it — the sim broker folds every generated fill into the
// ledger itself, so the sink would double-count. Call once before the engine starts
// processing events; a nil sink is ignored.
func (e *Engine) SetFillSink(s FillSink) {
	if s != nil {
		e.sink = s
	}
}

// logf writes one engine log line, tagged with the instance's LogTag so two strategies
// sharing a shared execengine.log (or a process) stay tellable apart — untagged, a HALTED
// or stray-fill line cannot be attributed to a bot.
func (e *Engine) logf(format string, args ...any) {
	mlog.Printf("[execengine]"+e.cfg.LogTag+" "+format, args...)
}

// placeBackoff is how long to suppress new clip opens after a placement failure or a
// rate-limiter denial with no known reset time.
func (e *Engine) placeBackoff() time.Duration {
	if e.cfg.PlaceRetryBackoff > 0 {
		return e.cfg.PlaceRetryBackoff
	}
	return 2 * time.Second
}

// OnTick drives the time-based backstops even when no market updates are arriving: the
// impaired-mode obligation retries, the OPTIONAL fill-timeout (a no-op with FillTimeout
// == 0, the default), the taker confirmation watchdog (see checkPendingTakers), and the
// stale-book order pull (live only, see PullOnStaleBook).
func (e *Engine) OnTick(now time.Time) {
	e.advanceNow(now)
	e.serviceImpaired()
	e.checkPendingTakers(e.now)
	e.checkStaleBooks(now)
	e.checkTimeout(now)
}

const (
	takerProbeEvery = 3 * time.Second // min gap between status polls of one overdue taker
	takerDeadLimit  = 3               // consecutive dead-short takers before slowing re-hedges to the impaired debt pace

	impairedRetryMax = time.Minute // obligation-retry gap ceiling while the broker keeps not answering
)

// OnFill folds an executed fill into the engine, keyed by the filled order's id, through
// the order's account: only lots the engine has not already acted on propagate. When the
// order is one of the working clip's passives, dual-passive first-fill-wins runs: the
// matched leg is the maker; its counterpart is retired (see retireOrder) and only the
// net gap hedged, committing the clip's direction. When the maker leg is fully filled
// the clip's intent is committed to the Decider's lot book.
//
// A fill on a RETIRED order is the event that raced its cancel: retireOrder already
// folded the broker's terminal executed count into the pair, so the event is a silent
// no-op up to that count (this replaces the old blind stray-hedge — the paid-alignment
// churn of 2026-07-15). Only lots EXCEEDING the terminal count (the broker contradicting
// its own cancel-ack) are hedged, loudly. Foreign order ids are no-ops; an own taker fill
// never pairs (a taker IS a hedge) and only trues its placement-time sink credit's price.
//
// Every own execution the engine acts on here is also credited to the fill sink exactly
// once (here and in finishRetire/tryPlaceTaker) — the live position source that cannot trail a stalled stream.
func (e *Engine) OnFill(ts time.Time, orderID, symbol string, buy bool, lots int, price float64) {
	e.advanceNow(ts)
	acct := e.own[orderID]
	if acct == nil || lots <= 0 {
		return // a foreign fill or an empty increment
	}
	// The order id alone binds the event to the order; the leg and side come from the
	// account — what the engine itself placed — never from the event. A fill event's symbol
	// can arrive in a different format than the config legs (the runners warn about exactly
	// that), and trusting it would make hedgeStrayMakerFill recognize neither leg and
	// silently skip a stray/excess hedge: a naked leg.
	if acct.sym != "" {
		symbol, buy = acct.sym, acct.isBuy
	}
	// An order can never execute more lots than it was placed for: reported lots beyond the
	// placed size are a re-delivered event that slipped the runner's trade-id dedup (or a
	// corrupt feed), not inventory. A live maker has no other duplicate guard, and on a
	// retired one the impossible lots would overflow past the terminal count into the
	// excess-hedge path — either way the engine would double-hedge and double-count the
	// position — so the impossible excess is dropped, loudly.
	if over := acct.seen + lots - acct.placed; over > 0 && acct.placed > 0 {
		e.logf("CRITICAL: %s reported %d lots beyond its placed size %d on %s — dropping the impossible excess (re-delivered fill?)", orderID, over, acct.placed, symbol)
		lots -= over
		if lots <= 0 {
			return
		}
	}
	reported := acct.seen
	acct.seen += lots
	if !acct.maker {
		// An own taker fill: a taker IS a hedge, so there is nothing to pair. Its lots were
		// credited to the sink at placement (see takerRetry); the event only trues the
		// crossed-touch estimate up to the actual fill price on the overlap. Lots beyond the
		// placement credit are NEVER credited: a taker cannot execute more than it was placed
		// for, so they can only be a re-delivered event (the runner's trade-id dedup is the
		// primary guard; this is the backstop that keeps a duplicate from minting inventory).
		if e.sink != nil {
			if pre := acct.credited - reported; pre > 0 && price != acct.price {
				e.sink.Amend(acct.sym, acct.isBuy, min(pre, lots), acct.price, price)
			}
		}
		// Fills past a CONFIRMED-DEAD taker's terminal count: the broker contradicting its
		// own terminal ack. These lots are real executions the settle already un-credited
		// (and re-hedged as a shortfall), so the book is now over-hedged by them — re-credit
		// the truth and say so loudly; reconcile confirms which legs are actually unbalanced.
		if acct.final >= 0 {
			if beyond := acct.seen - max(acct.final, reported); beyond > 0 {
				e.logf("CRITICAL: taker %s filled %d lots BEYOND its confirmed terminal count %d on %s — the broker contradicts its own terminal ack; the book may be over-hedged (reconcile will confirm)", orderID, beyond, acct.final, symbol)
				if e.sink != nil {
					e.sink.Fill(acct.sym, acct.isBuy, beyond, price)
					acct.credited += beyond
				}
			}
		}
		return
	}
	if acct.final >= 0 {
		// Lots in this event beyond BOTH the terminal count and everything already reported.
		// The cumulative view (no seen-rollback — the old rollback re-hedged the SAME
		// beyond-terminal lots on every re-delivery, unboundedly) caps the total ever hedged
		// beyond the ack at placed − final: a genuine beyond-terminal fill — the broker
		// contradicting its own cancel-ack — is hedged below, while endless re-deliveries are
		// cut off at the placed-size ceiling above.
		excess := acct.seen - max(acct.final, reported)
		if excess < 0 {
			excess = 0
		}
		// The sink credit MIRRORS the hedge decision exactly, so the ledger position and the
		// hedged book can never diverge: the event's overlap with the retire credit (dup) only
		// trues the limit-price estimate — a no-op for a passive, which fills at its limit —
		// while excess lots (hedged below) are new real inventory and credit as a fresh fill.
		if e.sink != nil {
			if dup := lots - excess; dup > 0 && price != acct.price {
				e.sink.Amend(acct.sym, acct.isBuy, dup, acct.price, price)
			}
			if excess > 0 {
				e.sink.Fill(acct.sym, acct.isBuy, excess, price)
				acct.credited += excess
			}
		}
		if excess > 0 {
			e.logf("CRITICAL: %s filled %d lots BEYOND its terminal executed count on %s — pair-hedging the excess", orderID, excess, symbol)
			e.hedgeStrayMakerFill(orderID, symbol, buy, excess)
		}
		return // already acted on via retireOrder — the dedup that kills the churn
	}
	// A live (never-retired) maker order: every reported lot is new to the sink — nothing
	// can have been credited ahead of the stream for it (retire and taker placement are the
	// only ahead-of-stream credits, and neither applies here).
	if e.sink != nil {
		e.sink.Fill(acct.sym, acct.isBuy, lots, price)
		acct.credited += lots
	}
	acct.folded += lots

	c := e.clip
	if c == nil || (orderID != c.legA.id && orderID != c.legB.id) {
		// A LIVE maker order outside the working clip should not exist (every drop path
		// retires) — hedge defensively rather than leave a naked leg.
		e.logf("stray passive fill: %s filled %d on %s with no owning clip — hedging", orderID, lots, symbol)
		e.hedgeStrayMakerFill(orderID, symbol, buy, lots)
		return
	}
	firstFill := c.makerID == ""
	if firstFill {
		c.makerID = orderID // the first leg to fill is the maker
	}
	if orderID != c.makerID {
		// The non-maker leg filled after we designated the maker but BEFORE its retire ran
		// (events interleaved inside one loop turn). Real inventory — hedge the increment.
		e.logf("race: non-maker leg %s filled %d — hedging the increment", symbol, lots)
		e.hedgeStrayMakerFill(orderID, symbol, buy, lots)
		return
	}
	// Resolve the pair to 1:1. On the FIRST fill, retire the counterpart synchronously:
	// its gap is its full executed count (no counterpart fill events can have been folded
	// before the maker was designated — the first processed fill IS the maker), and the
	// pair lands at max(makerFilled, gap) ≤ target — never doubling when both legs fill.
	// Late counterpart fill events then dedup against its account instead of re-hedging.
	// On later maker fills the counterpart is already retired, so hedge the increment.
	c.makerFilled += lots
	if firstFill {
		cp, cpSym := &c.legB, e.cfg.LegB
		if orderID != c.legA.id {
			cp, cpSym = &c.legA, e.cfg.LegA
		}
		executed := e.retireOrder(cp.id)
		if executed > c.makerFilled {
			// The counterpart ran ahead of the maker's first fill. The old handling (taker
			// top-up on the maker's leg, clip kept working) double-realized the top-up lots
			// whenever the maker's passive — still resting at the broker for its FULL
			// remainder — later filled past target − topUp: a target-4 clip could land 6:6,
			// blowing past the cap and desyncing the Decider's book from the broker (a
			// guaranteed two-pass reconcile halt). The pair is resolved NOW instead, by the
			// same convention resolveClip applies to a partial: complete to target with
			// takers (a close always completes), unless an opening clip prefers keeping the
			// already-realized partial (KeepPartialOpenOnTimeout) — then the legs are only
			// equalized at the larger side and the clip is dropped uncommitted.
			realA, realB := c.makerFilled, executed
			if cp == &c.legA {
				realA, realB = executed, c.makerFilled
			}
			if !c.intent.IsClose && e.cfg.KeepPartialOpenOnTimeout {
				e.settleClip(realA, realB, false)
				e.clip = nil
				return
			}
			e.settleClip(realA, realB, true)
			if !e.recovery.halted {
				e.commitClip(ts)
			}
			return
		}
		if executed < c.makerFilled {
			// Top the counterpart up to the maker, in the counterpart's own contracts (see
			// hedgeLots — identity unless this is LegB at a hedge ratio).
			e.takerRetry(cpSym, cp.isBid, e.hedgeLots(cpSym, c.makerFilled-executed))
		}
	} else if orderID == c.legA.id {
		e.hedge(e.cfg.LegB, c.dir < 0, e.hedgeLots(e.cfg.LegB, lots))
	} else {
		e.hedge(e.cfg.LegA, c.dir > 0, lots)
	}
	if e.recovery.halted {
		return // a taker failed → already halted and resting passives cancelled
	}
	if c.makerFilled >= c.target {
		e.commitClip(ts) // fully filled — book the lot
	}
}

// Position returns the current position in contracts (+ = long leg A / short leg B).
func (e *Engine) Position() int { return e.dm.Position() }

// Working reports whether a clip's passive orders are currently in flight.
func (e *Engine) Working() bool { return e.clip != nil }
