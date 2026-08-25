package finam

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/marketdata"
	"google.golang.org/grpc/metadata"
)

const (
	orderBookChannelBufferSize = 10000 // ~100-1000s of book ticks: rides out any realistic consumer stall without evicting (only a full buffer drops, oldest first)
	reconnectDelay             = 1 * time.Second
	dropWarnInterval           = 5 * time.Second // min gap between consumer-lagging warnings per stream
)

// heartbeatTimeout is the DEFAULT for how long streamBook tolerates a subscription
// sending no data before treating it as dead and forcing a reconnect — used whenever a
// Ticker doesn't set its own HeartbeatTimeout. It is a var, not a const, so tests can
// shrink it; production always runs with the value below.
//
// Was 30s until 2026-07-30, then 5min until 2026-08-12. The 30s→5min raise (basis-live-vs-
// backtest-0729 gap investigation) fixed excess overnight reconnects: the listener's
// LEGA/LEGB subscriptions were hitting 30s during genuinely quiet (not dead) periods
// outside the liquid session, and because the bot's own subscription and the listener's are
// two INDEPENDENT connections, their reconnect blind windows didn't line up — 00:00-04:06 UTC
// on 2026-07-29 the persisted store recorded ~45% fewer signal folds than the bot's own trace.
//
// 5min→10s on 2026-08-12: a live incident showed the failure mode that matters more for a
// live trading leg. At 12:52:32 UTC LEGB@RTSX got a broker-initiated GOAWAY
// (graceful_shutdown), reconnected with no further logged error, then delivered ZERO further
// book updates for 4m8s straight (confirmed via basis_trace.log — perp_bid/perp_ask frozen
// bit-for-bit) until the process was killed manually, 52s short of the old 5min timeout
// firing on its own. This is a stream that is alive at the transport level (the HTTP/2 PING
// keepalive in streamDialOpts, auth.go, would not have caught it — pings were ACKed fine on
// 2026-07-30 precisely because the socket itself stays up) but stuck at the
// subscription/data level after a broker-side reconnect. bookFresh's 3s staleness gate
// (strategies/basis) kept the decider in hold=stale throughout, so this did not cause a bad
// fill — but it did leave the bot unable to trade for over 4 minutes on a live leg, and (see
// that same incident's postmortem) a manual kill+restart to "fix" it costs a 5-6 minute
// cold-start EWM re-warm that trusting the automatic reconnect would not have — the zombie
// leg was 52s from self-healing on its own when it got killed instead.
//
// This is why the two conflicting tradeoffs (2026-07-29's "don't reconnect too eagerly" vs.
// 2026-08-12's "detect a zombie fast") are no longer forced through one shared number:
// trading callers (basis, spread — anything where minutes untradeable cost money) get this
// 10s default; the recording listener, which cares more about not shredding overnight
// coverage than about a stale leg (it never trades), sets Ticker.HeartbeatTimeout to its own
// longer value — see listener.listenerHeartbeatTimeout.
var heartbeatTimeout = 10 * time.Second

type PriceLevel struct {
	Price  float64
	Volume float64
}

type FullOrderBookData struct {
	Timestamp time.Time
	Symbol    string
	Bids      []PriceLevel
	Asks      []PriceLevel
	// Drops is how many snapshots this subscription had discarded (see sendKeepNewest) BEFORE
	// this one. A consumer that sees it rise between two snapshots knows the book it now holds
	// does not continue the book it held — the state in between is gone. Nothing about the
	// snapshot itself reveals this: it is valid and recent either way.
	Drops int64
}

type Ticker struct {
	Symbol string
	Vol    int
	// HeartbeatTimeout overrides the package-default heartbeatTimeout for this
	// subscription's streamBook loop. Zero means "use the default" (currently tuned
	// for live trading legs, where every second spent undetected on a zombie
	// subscription is a second the leg can't trade). Callers for whom a reconnect is
	// costlier than staying briefly blind — the recording listener's overnight-quiet
	// tradeoff, see listener.listenerHeartbeatTimeout — set this explicitly instead of
	// living with the trading default.
	HeartbeatTimeout time.Duration
}

type OrderBookData struct {
	Timestamp time.Time
	Symbol    string
	BestBid   float64
	BestAsk   float64
}

func calculateExecutionPrice(priceLevels []PriceLevel, targetVolume int) (float64, bool) {
	if len(priceLevels) == 0 || targetVolume <= 0 {
		return 0, false
	}

	var (
		accumulatedVolume float64
		executionPrice    float64
	)

	targetVol := float64(targetVolume)

	for _, level := range priceLevels {
		remainingVolume := targetVol - accumulatedVolume
		if remainingVolume <= 0 {
			break
		}

		volumeToTake := level.Volume
		if volumeToTake > remainingVolume {
			volumeToTake = remainingVolume
		}

		accumulatedVolume += volumeToTake
		executionPrice += level.Price * volumeToTake
	}

	if accumulatedVolume > 0 {
		return executionPrice / accumulatedVolume, true
	}

	return 0, false
}

// applyRow folds one order-book delta into the bid/ask price→volume maps.
// GetLastQuote returns the broker's last known quote for symbol: top of book plus the
// session aggregates (volume, turnover, open interest). It is a snapshot RPC, not a
// stream — use it to size or screen an instrument, not to trade off.
func GetLastQuote(ctx context.Context, client *Client, symbol string) (*marketdata.Quote, error) {
	conn, rctx, cancel, err := client.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	resp, err := marketdata.NewMarketDataServiceClient(conn).LastQuote(rctx, &marketdata.QuoteRequest{
		Symbol: symbol,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetQuote(), nil
}

func applyRow(row *marketdata.StreamOrderBook_Row, bidBook, askBook map[float64]float64, symbol string) {
	if row == nil {
		return
	}
	if row.Price == nil {
		return
	}

	price, err := strconv.ParseFloat(row.Price.Value, 64)
	if err != nil {
		mlog.Printf("[%s] Error converting price to float64: %v", symbol, err)
		return
	}

	apply := func(book map[float64]float64, sizeValue string, label string) {
		switch row.Action {
		case marketdata.StreamOrderBook_Row_ACTION_REMOVE:
			delete(book, price)
		case marketdata.StreamOrderBook_Row_ACTION_ADD, marketdata.StreamOrderBook_Row_ACTION_UPDATE:
			volume, err := strconv.ParseFloat(sizeValue, 64)
			if err != nil {
				mlog.Printf("[%s] Error converting %s to float64: %v", symbol, label, err)
				return
			}
			if volume <= 0 {
				delete(book, price)
			} else {
				book[price] = volume
			}
		}
	}

	if buySize := row.GetBuySize(); buySize != nil {
		apply(bidBook, buySize.Value, "buy size")
	}
	if sellSize := row.GetSellSize(); sellSize != nil {
		apply(askBook, sellSize.Value, "sell size")
	}
}

// sortedLevels materializes the non-empty levels of a book, sorted by the given
// price ordering (descending for bids, ascending for asks).
func sortedLevels(book map[float64]float64, descending bool) []PriceLevel {
	levels := make([]PriceLevel, 0, len(book))
	for price, volume := range book {
		if volume > 0 {
			levels = append(levels, PriceLevel{Price: price, Volume: volume})
		}
	}
	sort.Slice(levels, func(i, j int) bool {
		if descending {
			return levels[i].Price > levels[j].Price
		}
		return levels[i].Price < levels[j].Price
	})
	return levels
}

// crossed reports whether the best bid is strictly above the best ask — a corrupt
// book state that incremental deltas can produce when a level's REMOVE is missed,
// leaving a stale level that never clears on its own. bids/asks are as sortedLevels
// yields them (bids descending, asks ascending), so index 0 is the best on each side.
func crossed(bids, asks []PriceLevel) bool {
	return len(bids) > 0 && len(asks) > 0 && bids[0].Price > asks[0].Price
}

// streamBook opens an order-book stream for ticker and invokes onSnapshot with the
// freshly rebuilt, sorted bid/ask sides after every update. It returns on any stream
// error or heartbeat timeout so the caller's reconnect loop can re-establish it.
func streamBook(client *Client, ticker Ticker, onSnapshot func(timestamp time.Time, symbol string, bids, asks []PriceLevel)) error {
	conn, err := client.GetStreamConn(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", client.GetJWT())

	mdClient := marketdata.NewMarketDataServiceClient(conn)
	stream, err := mdClient.SubscribeOrderBook(ctx, &marketdata.SubscribeOrderBookRequest{
		Symbol: ticker.Symbol,
	})
	if err != nil {
		return err
	}

	timeout := heartbeatTimeout
	if ticker.HeartbeatTimeout > 0 {
		timeout = ticker.HeartbeatTimeout
	}

	askBook := make(map[float64]float64)
	bidBook := make(map[float64]float64)

	respChan := make(chan *marketdata.SubscribeOrderBookResponse, 1)
	errChan := make(chan error, 1)

	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case respChan <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var crossedCount int
	const maxCrossedResync = 50 // consecutive crossed snapshots before forcing a resync

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timer.C:
			mlog.Printf("[%s] Heartbeat timeout - no data received for %v", ticker.Symbol, timeout)
			return fmt.Errorf("heartbeat timeout: no data received for %v", timeout)

		case err := <-errChan:
			return err

		case resp := <-respChan:
			timer.Stop()
			timer.Reset(timeout)

			timestamp := time.Now().UTC()

			for _, orderBook := range resp.OrderBook {
				for _, row := range orderBook.Rows {
					applyRow(row, bidBook, askBook, ticker.Symbol)
				}

				bids := sortedLevels(bidBook, true)
				asks := sortedLevels(askBook, false)

				if crossed(bids, asks) {
					// A stale level is crossing the opposite side. Don't emit the
					// garbage snapshot; if it persists the book won't self-heal, so
					// force a reconnect to re-subscribe with a fresh book.
					crossedCount++
					if crossedCount == 1 {
						mlog.Printf("[%s] Crossed book: best bid %.4f > best ask %.4f (skipping until it clears)",
							orderBook.Symbol, bids[0].Price, asks[0].Price)
					}
					if crossedCount >= maxCrossedResync {
						return fmt.Errorf("crossed book persisted for %d snapshots, forcing resync", crossedCount)
					}
					continue
				}
				crossedCount = 0

				onSnapshot(timestamp, orderBook.Symbol, bids, asks)
			}
		}
	}
}

// sendKeepNewest delivers v to ch without ever blocking the stream reader. A book
// snapshot is complete state, not an event — the newest supersedes everything queued
// before it — so when the buffer is full the OLDEST queued item is discarded to make
// room. (The previous policy dropped the NEWEST update, leaving a lagging consumer to
// chew through a full buffer of stale books while fresh ones were thrown away, so the
// strategy kept acting on prices that were seconds old.) It reports whether anything
// was dropped so the caller can warn, rate-limited.
func sendKeepNewest[T any](ch chan T, v T) (dropped bool) {
	for {
		select {
		case ch <- v:
			return dropped
		default:
		}
		select {
		case <-ch:
			dropped = true
		default: // consumer drained concurrently — retry the send
		}
	}
}

// dropWarner rate-limits the consumer-lagging warning: one line per dropWarnInterval
// carrying the accumulated drop count, instead of one line per dropped snapshot (the
// per-drop spam itself added enough stdout pressure to keep the consumer behind).
type dropWarner struct {
	symbol string
	what   string
	// dropped is the rate-limiter's own accumulator: it resets every time a warning prints.
	// total never resets — it is stamped onto outgoing snapshots so a consumer can tell that
	// the book it just received does not continue the one before it (see FullOrderBookData.Drops).
	dropped  int
	total    int64
	lastWarn time.Time
}

func (w *dropWarner) note() {
	w.dropped++
	w.total++
	if now := time.Now(); w.lastWarn.IsZero() || now.Sub(w.lastWarn) >= dropWarnInterval {
		mlog.Printf("[%s] Warning: %s consumer lagging — dropped %d oldest queued snapshot(s), keeping the newest",
			w.symbol, w.what, w.dropped)
		w.dropped = 0
		w.lastWarn = now
	}
}

func subscribeOrderBook(client *Client, ticker Ticker, dataChan chan OrderBookData) error {
	drops := dropWarner{symbol: ticker.Symbol, what: "order book"}
	return streamBook(client, ticker, func(timestamp time.Time, symbol string, bids, asks []PriceLevel) {
		bestAsk, hasAsk := calculateExecutionPrice(asks, ticker.Vol)
		bestBid, hasBid := calculateExecutionPrice(bids, ticker.Vol)
		if !hasBid || !hasAsk {
			return
		}

		data := OrderBookData{
			Timestamp: timestamp,
			Symbol:    symbol,
			BestBid:   bestBid,
			BestAsk:   bestAsk,
		}

		if sendKeepNewest(dataChan, data) {
			drops.note()
		}
	})
}

func subscribeFullOrderBook(client *Client, ticker Ticker, dataChan chan FullOrderBookData) error {
	drops := dropWarner{symbol: ticker.Symbol, what: "full orderbook"}
	return streamBook(client, ticker, func(timestamp time.Time, symbol string, bids, asks []PriceLevel) {
		if len(bids) == 0 || len(asks) == 0 {
			return
		}

		data := FullOrderBookData{
			Timestamp: timestamp,
			Symbol:    symbol,
			Bids:      bids,
			Asks:      asks,
			Drops:     drops.total, // drops BEFORE this snapshot; the consumer diffs consecutive values
		}

		if sendKeepNewest(dataChan, data) {
			drops.note()
		}
	})
}

// subscribeWithReconnect runs a streaming subscription on a background goroutine,
// reconnecting after a fixed delay whenever it returns an error.
func subscribeWithReconnect[T any](client *Client, ticker Ticker, run func(*Client, Ticker, chan T) error) <-chan T {
	dataChan := make(chan T, orderBookChannelBufferSize)

	go func() {
		defer close(dataChan)
		reconnectLoop(ticker.Symbol, func() error { return run(client, ticker, dataChan) })
	}()

	return dataChan
}

func SubscribeOrderBook(client *Client, ticker Ticker) (<-chan OrderBookData, error) {
	return subscribeWithReconnect(client, ticker, subscribeOrderBook), nil
}

func SubscribeFullOrderBook(client *Client, ticker Ticker) (<-chan FullOrderBookData, error) {
	return subscribeWithReconnect(client, ticker, subscribeFullOrderBook), nil
}

// SubscribeLatestTrades streams the public trade tape (time-and-sales) for symbol
// from the market-data service, reconnecting on any stream error. Intended for
// logging and analytics; distinct from SubscribeTrades, which streams the
// account's OWN fills.
func SubscribeLatestTrades(client *Client, symbol string) (<-chan *marketdata.Trade, error) {
	out := make(chan *marketdata.Trade, orderBookChannelBufferSize)
	go func() {
		defer close(out)
		reconnectLoop("latest-trades", func() error { return runLatestTradesStream(client, symbol, out) })
	}()
	return out, nil
}

func runLatestTradesStream(client *Client, symbol string, out chan<- *marketdata.Trade) error {
	conn, err := client.GetStreamConn(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", client.GetJWT())

	mdClient := marketdata.NewMarketDataServiceClient(conn)
	stream, err := mdClient.SubscribeLatestTrades(ctx, &marketdata.SubscribeLatestTradesRequest{
		Symbol: symbol,
	})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		for _, tr := range resp.Trades {
			select {
			case out <- tr:
			default:
				mlog.Printf("[%s] Warning: latest-trades channel full, dropping update", symbol)
			}
		}
	}
}
