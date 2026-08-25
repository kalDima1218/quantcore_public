package execengine

// TradeDedup remembers account-trade ids so a fill re-delivered by a stream reconnect
// is folded only once. Process-lifetime, like Engine.own: dropping an id early would
// risk double-counting a late re-delivery. The zero value is ready to use — including
// `var d TradeDedup`, not just the `TradeDedup{}` composite literal every current call
// site happens to use.
type TradeDedup struct {
	seen map[string]struct{}
}

// Seen reports whether tid was already recorded, recording it otherwise. An empty id
// (no dedup key) is never treated as a duplicate.
func (d *TradeDedup) Seen(tid string) bool {
	if tid == "" {
		return false
	}
	if _, dup := d.seen[tid]; dup {
		return true
	}
	if d.seen == nil {
		d.seen = make(map[string]struct{})
	}
	d.seen[tid] = struct{}{}
	return false
}
