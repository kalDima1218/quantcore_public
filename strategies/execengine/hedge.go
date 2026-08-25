// pendingSet is the defined type of Engine.pending: the set of own taker order ids whose
// placement credit the broker has NOT yet confirmed (fill events covering the placed
// size, or a terminal status). The engine's model books a placed taker as done, but that
// is an ASSUMPTION until data confirms it: past TakerConfirmTimeout the engine stops
// opening new clips on top of the unconfirmed hedge and polls the order's status until
// the broker answers (see checkPendingTakers).
//
// Like recovery and ledger (see recovery.go, ledger.go), this is a state grouping, not a
// black-box component: placeTakerRPC's caller mutates pending alongside the ledger, the
// sink and the quota limiter within a single placement, so those transitions stay on
// Engine (engine_hedge.go) and index e.pending directly.
package execengine

type pendingSet map[string]struct{}
