// Package hedgemanager holds PendingSet: the set of own taker order ids whose placement
// credit the broker has NOT yet confirmed (fill events covering the placed size, or a
// terminal status). The engine's model books a placed taker as done, but that is an
// ASSUMPTION until data confirms it: past TakerConfirmTimeout the engine stops opening new
// clips on top of the unconfirmed hedge and polls the order's status until the broker
// answers (see Engine.checkPendingTakers).
//
// Like orderregistry.Ledger, this is a plain data type, not a rich behavior-owning
// component: the dependency mapping for this split (see quotebook/recoverymachine's commit
// messages) found the methods that read and write it — checkPendingTakers, settleTaker,
// commitTakerPlacement, confirmDeadTaker, awaitingTaker — reach into ledger+recovery+quote
// state within single events (a placement credits the ledger, the sink, the quota limiter
// and this set all together) or require the broker/sink/limiter/clock ports Engine itself
// owns. Moving that orchestration here would mean re-injecting Maker/Taker/FillSink/
// Limiter/Clock into a second place — a materially larger, riskier redesign than what made
// quotebook/recoverymachine/orderregistry clean leaves, not a proportionate next step. It
// stays on Engine (engine_hedge.go), same conclusion as clip/legOrder in
// strategies/execengine/CLAUDE.md's Engine-decomposition note.
package hedgemanager

// PendingSet is the defined type of Engine.pending — see the package doc comment. A plain
// map type: Go's map operations (index, assign, delete, len, range) work identically
// regardless of which package declared it, so callers use it exactly like the map it is —
// no accessor layer needed for that part.
type PendingSet map[string]struct{}
