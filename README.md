# QuantCore — execution infrastructure (extracted subset)

This is a scrubbed, self-contained extract of four packages from a private trading
monorepo, for external code review only. It is **not** the full project and there is
no `package main` here — nothing runs standalone, but `go build ./...`, `go vet ./...`
and `go test ./...` all pass.

## What's here

- `strategies/execengine` — the order-execution engine: passive limit orders, taker
  hedging on fill, re-peg, cancels, reconcile-after-reconnect, an impaired/halted mode
  for broker trouble. It knows nothing about trading signals — it takes an `Intent`
  (direction + size for two legs, "LegA"/"LegB") from a caller and executes it.
- `trade/finam` — a client for a real broker's public gRPC trading API (auth, orders,
  account/margin, order book streaming, market-schedule polling).
- `grpcclient` — a small generic gRPC connection wrapper (reconnect/backoff) used by
  the above.
- `modlog` — a trivial per-module file+stderr logger.

## What's deliberately NOT here

The actual trading strategy (signal construction, entry/exit thresholds, position
sizing, which instruments are paired) lives in separate packages that are excluded
from this extract on purpose. The only thing safe to say about it here is that it's a
two-leg (pair) strategy — a "dated" leg and a "perp" leg, referred to generically as
LegA/LegB throughout this code, same as `execengine`'s own naming. Real instrument
tickers, spread economics, and account identifiers have been replaced with generic
placeholders (`LEGA@RTSX`/`LEGB@RTSX`) in test fixtures and comments.
