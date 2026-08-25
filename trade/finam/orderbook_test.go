package finam

import (
	"net"
	"testing"
	"time"

	"QuantCore/grpcclient"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/marketdata"
	"google.golang.org/grpc"
)

// quietOrderBookServer starts a gRPC market-data server whose SubscribeOrderBook sends
// exactly one snapshot and then goes silent — holding the stream open (no error, no more
// data) until the client disconnects. This is the exact shape of a genuinely quiet market
// (an idle overnight book): the CONNECTION stays healthy, only application data stops.
func quietOrderBookServer(t *testing.T, symbol string) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	marketdata.RegisterMarketDataServiceServer(srv, &quietOBServer{symbol: symbol})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

type quietOBServer struct {
	marketdata.UnimplementedMarketDataServiceServer
	symbol string
}

func (s *quietOBServer) SubscribeOrderBook(_ *marketdata.SubscribeOrderBookRequest, stream marketdata.MarketDataService_SubscribeOrderBookServer) error {
	if err := stream.Send(&marketdata.SubscribeOrderBookResponse{
		OrderBook: []*marketdata.StreamOrderBook{{Symbol: s.symbol}},
	}); err != nil {
		return err
	}
	<-stream.Context().Done() // one snapshot, then silence until the caller disconnects
	return stream.Context().Err()
}

// TestStreamBookToleratesConfiguredQuietPeriod pins the property the fix for the 2026-07-29
// live/backtest gap depends on: streamBook must wait the FULL configured heartbeatTimeout
// before treating silence as a dead connection, not fire early. Overlaying the live bot's own
// trace against a replay of the persisted market-data logstore showed the listener's
// connection was reconnecting every 10-30s overnight purely from quiet-market silence — a
// heartbeat that fires before its own timeout elapses would make that churn worse, not better.
// heartbeatTimeout is a package var (not a const) specifically so this test can shrink it to
// run in milliseconds instead of the real several-minute production value.
func TestStreamBookToleratesConfiguredQuietPeriod(t *testing.T) {
	orig := heartbeatTimeout
	heartbeatTimeout = 150 * time.Millisecond
	t.Cleanup(func() { heartbeatTimeout = orig })

	addr := quietOrderBookServer(t, "LEGA@RTSX")
	gc, err := grpcclient.NewClientInsecure(addr, streamDialOpts()...)
	if err != nil {
		t.Fatalf("dialing the quiet server: %v", err)
	}
	t.Cleanup(func() { _ = gc.Close() })

	client := &Client{streamClient: gc}

	errCh := make(chan error, 1)
	go func() {
		errCh <- streamBook(client, Ticker{Symbol: "LEGA@RTSX"}, func(time.Time, string, []PriceLevel, []PriceLevel) {})
	}()

	// Well within heartbeatTimeout: the connection is healthy and merely quiet, so
	// streamBook must still be running.
	select {
	case err := <-errCh:
		t.Fatalf("streamBook returned (%v) after only %v of a %v heartbeatTimeout — "+
			"a healthy-but-quiet stream must not be treated as dead early", err, heartbeatTimeout/3, heartbeatTimeout)
	case <-time.After(heartbeatTimeout / 3):
	}

	// Comfortably past heartbeatTimeout with no further data: streamBook must now return.
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("streamBook returned nil error past heartbeatTimeout with no data — want the heartbeat-timeout error")
		}
	case <-time.After(heartbeatTimeout * 5):
		t.Fatalf("streamBook did not return within %v of true silence past heartbeatTimeout=%v", heartbeatTimeout*5, heartbeatTimeout)
	}
}

// TestStreamBookUsesPerTickerHeartbeatTimeoutOverride pins Ticker.HeartbeatTimeout as the
// per-caller override added alongside the 2026-08-12 5min->10s trading-default change: the
// recording listener needs a LONGER timeout than a live trading leg (see
// listener.listenerHeartbeatTimeout), so streamBook must honor a Ticker-level value instead
// of always falling back to the package default. Package heartbeatTimeout is set deliberately
// LONG here — if the override were ignored, streamBook would still be blocked on it and this
// test would time out waiting for a return that never (promptly) comes.
func TestStreamBookUsesPerTickerHeartbeatTimeoutOverride(t *testing.T) {
	orig := heartbeatTimeout
	heartbeatTimeout = time.Hour
	t.Cleanup(func() { heartbeatTimeout = orig })

	const override = 150 * time.Millisecond

	addr := quietOrderBookServer(t, "LEGA@RTSX")
	gc, err := grpcclient.NewClientInsecure(addr, streamDialOpts()...)
	if err != nil {
		t.Fatalf("dialing the quiet server: %v", err)
	}
	t.Cleanup(func() { _ = gc.Close() })

	client := &Client{streamClient: gc}

	errCh := make(chan error, 1)
	go func() {
		errCh <- streamBook(client, Ticker{Symbol: "LEGA@RTSX", HeartbeatTimeout: override}, func(time.Time, string, []PriceLevel, []PriceLevel) {})
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("streamBook returned nil error past the ticker's HeartbeatTimeout override with no data — want the heartbeat-timeout error")
		}
	case <-time.After(override * 20):
		t.Fatalf("streamBook did not return within %v — the Ticker.HeartbeatTimeout override (%v) was not honored, "+
			"it fell back to the hour-long package default", override*20, override)
	}
}

// A dropped snapshot is invisible downstream: the consumer receives a valid, recent book whose
// trajectory has a hole, and no existing check can see it (the 2026-07-24 churn). dropWarner must
// therefore carry a MONOTONIC total the stream can stamp onto outgoing snapshots — distinct from
// `dropped`, which is the rate-limiter's own counter and resets every time a warning prints.
func TestDropWarnerTotalIsMonotonic(t *testing.T) {
	w := dropWarner{symbol: "LEGA@RTSX", what: "full orderbook"}
	for i := 0; i < 5; i++ {
		w.note()
	}
	if w.total != 5 {
		t.Fatalf("total=%d want 5 — every drop must be counted for the stamp", w.total)
	}
	if int64(w.dropped) == w.total {
		t.Errorf("dropped=%d must stay the rate-limited counter (it resets on a printed warning), not a second total", w.dropped)
	}
}

// crossed detects a corrupt book where the best bid sits above the best ask.
// Levels arrive as sortedLevels produces them: bids descending, asks ascending,
// so index 0 is the best on each side.
func TestCrossed(t *testing.T) {
	cases := []struct {
		name       string
		bids, asks []PriceLevel
		want       bool
	}{
		{"normal", []PriceLevel{{101, 5}, {100, 3}}, []PriceLevel{{102, 4}, {103, 2}}, false},
		{"crossed", []PriceLevel{{42864, 1}, {42863, 1}}, []PriceLevel{{42761, 1}, {42772, 1}}, true},
		{"locked equal is not crossed", []PriceLevel{{100, 1}}, []PriceLevel{{100, 1}}, false},
		{"empty bids", nil, []PriceLevel{{100, 1}}, false},
		{"empty asks", []PriceLevel{{100, 1}}, nil, false},
	}
	for _, c := range cases {
		if got := crossed(c.bids, c.asks); got != c.want {
			t.Errorf("%s: crossed=%v want %v", c.name, got, c.want)
		}
	}
}

// sendKeepNewest must never block the stream reader and, on a full buffer, must
// evict the OLDEST queued snapshot so a lagging consumer resumes from fresh state —
// the opposite of the old drop-the-newest policy behind the "full orderbook channel
// full" warnings.
func TestSendKeepNewestEvictsOldest(t *testing.T) {
	ch := make(chan int, 3)
	for i := 1; i <= 3; i++ {
		if dropped := sendKeepNewest(ch, i); dropped {
			t.Fatalf("send %d into non-full buffer reported a drop", i)
		}
	}
	if dropped := sendKeepNewest(ch, 4); !dropped {
		t.Fatal("send into a full buffer should report a drop")
	}
	var got []int
	for len(ch) > 0 {
		got = append(got, <-ch)
	}
	want := []int{2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("queued=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("queued=%v want %v (oldest must be evicted, newest kept)", got, want)
		}
	}
}
