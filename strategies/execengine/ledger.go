// ledger maps every order id this engine has placed THIS process to its fill account
// (see ordAcct): maker-ness, lots acted on, and the terminal executed count learned at
// retirement. It is process-scoped: it starts empty on every restart. The live runner
// gates its ledger fold on Own(id) so that (a) fills from other strategies sharing the
// account and (b) this strategy's OWN fills from a prior session (replayed by the
// account trade stream on subscribe, already captured by the broker-position seed) are
// not folded into the position — only genuinely-own fills of the current session move
// it. The per-order account is what lets OnFill dedup a fill that raced its cancel-ack
// (retireOrder already acted on those lots) instead of blindly re-hedging it, without
// ever re-hedging a taker fill (which IS a hedge). Backtests never consult Own (the sim
// broker only ever generates own fills), so the gate is inert there.
//
// It is append-only within a process: an id is never removed after its order fills or
// cancels, because a late race-fill on a just-cancelled (re-pegged) leg is still a genuine
// own fill that must dedup against its account. Growth is bounded by the daily process
// restart (a day of order ids is a few MB at most); a TTL prune is deliberately avoided as
// it would risk dropping a late own fill and under-counting the position.
//
// Like recovery (see recovery.go), this is a state grouping, not a black-box component:
// OnFill and OnOrderStatus read and write ledger entries alongside clip/hedge/recovery
// state within a single event, so those transitions stay on Engine and index e.own
// directly — a defined map type is enough to make ownership explicit without adding an
// indirection hop to every read.
package execengine

import "time"

// ordAcct is one own order's fill account: whether it was placed as a clip's passive
// maker leg (vs a taker hedge), how many of its lots the engine has ACTED ON (folded:
// live fill events plus retire gaps), how many the fill stream has REPORTED (seen — the
// dedup guard against acting twice on the same lots), and the authoritative total it had
// executed when it was retired (final; -1 while the order may still fill).
//
// placed is the order's placement size — the hard ceiling on everything above: an order
// can never execute more lots than it was placed for, so reported fills and terminal
// executed counts beyond it are broker/feed corruption or re-delivered events, never
// inventory, and are clamped loudly instead of hedged (see OnFill and retireOrder). 0
// disables the clamp (only hand-built test accounts omit it).
//
// The order's leg, side and placement price are recorded so the sink credit paths
// (OnFill, finishRetire, tryPlaceTaker) can account a cancel-ack gap or a placed taker without a fill event: sym/isBuy
// name the inventory move, price is the credit-time price (a maker's resting limit — exact
// for a passive fill; a taker's crossed touch — an estimate the fill event later amends).
// credited counts the lots already handed to the sink, the guard that a late fill event
// re-prices rather than re-adds them.
type ordAcct struct {
	maker    bool
	sym      string
	isBuy    bool
	price    float64
	placed   int
	folded   int
	seen     int
	credited int
	final    int

	// Taker-confirmation state (takers only; see Engine.pending / checkPendingTakers).
	placedAt    time.Time // when the taker was placed (engine clock) — starts the confirm window
	lastProbe   time.Time // last Status poll, so an overdue taker is probed at takerProbeEvery, not every tick
	deadSeen    bool      // the order stream reported this taker terminal-dead but Status could not yet confirm the executed count — treat as overdue immediately
	probeLogged bool      // the one-shot "unconfirmed, polling" log line has been emitted

	// deferred (makers only): this order's retirement could not be confirmed and now lives
	// in recovery.retireQ — retireOrder short-circuits so the obligation loop is the only
	// path that keeps asking the broker about it.
	deferred bool
}

// ledger is the defined type of Engine.own — see the file doc comment above.
type ledger map[string]*ordAcct
