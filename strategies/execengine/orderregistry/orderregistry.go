// Package orderregistry holds one own order's fill account (OrdAcct) and the process-scoped
// ledger of them (Ledger). It is process-scoped: it starts empty on every restart. The live
// runner gates its ledger fold on Own(id) so that (a) fills from other strategies sharing
// the account and (b) this strategy's OWN fills from a prior session (replayed by the
// account trade stream on subscribe, already captured by the broker-position seed) are not
// folded into the position — only genuinely-own fills of the current session move it. The
// per-order account is what lets OnFill dedup a fill that raced its cancel-ack (retireOrder
// already acted on those lots) instead of blindly re-hedging it, without ever re-hedging a
// taker fill (which IS a hedge). Backtests never consult Own (the sim broker only ever
// generates own fills), so the gate is inert there.
//
// It is append-only within a process: an id is never removed after its order fills or
// cancels, because a late race-fill on a just-cancelled (re-pegged) leg is still a genuine
// own fill that must dedup against its account. Growth is bounded by the daily process
// restart (a day of order ids is a few MB at most); a TTL prune is deliberately avoided as
// it would risk dropping a late own fill and under-counting the position.
//
// Unlike quotebook/recoverymachine, OrdAcct's own fields are exported plain data (like
// quotebook.Touch), not method-wrapped: its every field is read and written by ONE tightly
// sequential algorithm (Engine.OnFill, ~170 lines) that depends on intermediate values
// across MULTIPLE fields together — per-field method wrapping would add a call layer around
// a straight-line imperative record update without protecting any invariant a single field
// owns on its own. That algorithm — and OnOrderStatus, retireOrder, finishRetire,
// tryDeferredRetire, confirmDeadTaker alongside it — stays on Engine: the dependency mapping
// for this split found their call graphs reach into ledger+clip+hedge+recovery state within
// single events (OnFill) or call onward into recoverymachine/hedge-retry machinery this
// package deliberately knows nothing about (deferRetire, hedgeStrayMakerFill). See
// strategies/execengine/CLAUDE.md's Engine-decomposition note.
package orderregistry

import "time"

// OrdAcct is one own order's fill account: whether it was placed as a clip's passive maker
// leg (vs a taker hedge), how many of its lots the engine has ACTED ON (Folded: live fill
// events plus retire gaps), how many the fill stream has REPORTED (Seen — the dedup guard
// against acting twice on the same lots), and the authoritative total it had executed when
// it was retired (Final; -1 while the order may still fill).
//
// Placed is the order's placement size — the hard ceiling on everything above: an order can
// never execute more lots than it was placed for, so reported fills and terminal executed
// counts beyond it are broker/feed corruption or re-delivered events, never inventory, and
// are clamped loudly instead of hedged (see Engine.OnFill and retireOrder). 0 disables the
// clamp (only hand-built test accounts omit it).
//
// The order's leg, side and placement price are recorded so the sink credit paths (OnFill,
// finishRetire, tryPlaceTaker) can account a cancel-ack gap or a placed taker without a fill
// event: Sym/IsBuy name the inventory move, Price is the credit-time price (a maker's
// resting limit — exact for a passive fill; a taker's crossed touch — an estimate the fill
// event later amends). Credited counts the lots already handed to the sink, the guard that a
// late fill event re-prices rather than re-adds them.
type OrdAcct struct {
	Maker    bool
	Sym      string
	IsBuy    bool
	Price    float64
	Placed   int
	Folded   int
	Seen     int
	Credited int
	Final    int

	// Taker-confirmation state (takers only; see Engine.pending / checkPendingTakers).
	PlacedAt    time.Time // when the taker was placed (engine clock) — starts the confirm window
	LastProbe   time.Time // last Status poll, so an overdue taker is probed at takerProbeEvery, not every tick
	DeadSeen    bool      // the order stream reported this taker terminal-dead but Status could not yet confirm the executed count — treat as overdue immediately
	ProbeLogged bool      // the one-shot "unconfirmed, polling" log line has been emitted

	// Deferred (makers only): this order's retirement could not be confirmed and now lives
	// in the recovery machine's retire queue — retireOrder short-circuits so the obligation
	// loop is the only path that keeps asking the broker about it.
	Deferred bool
}

// Confirmed reports whether a taker account's placement credit has been confirmed by data:
// the fill stream covered its placed size, or a terminal status settled it.
func (a *OrdAcct) Confirmed() bool { return a.Final >= 0 || a.Seen >= a.Placed }

// Ledger maps every order id this engine has placed THIS process to its fill account — see
// the package doc comment. A plain map type: Go's map operations (index, assign, delete,
// len, range) work identically regardless of which package declared it, so callers use it
// exactly like the map it is — no accessor layer needed for that part.
type Ledger map[string]*OrdAcct
