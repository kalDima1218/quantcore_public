package finam

import (
	"QuantCore/grpcclient"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

const (
	authTimeout    = 30 * time.Second
	authRetryDelay = 1 * time.Second
	// unaryRPCTimeout bounds EVERY unary broker RPC end to end — waiting for the
	// connection to become ready plus the round trip itself (see dial). Without it a
	// broker that accepts a request and then never answers parks the calling goroutine
	// forever: the socket stays ESTABLISHED, so nothing ever errors out. That is not
	// hypothetical — on 2026-07-29 the engine goroutine sat 44 minutes in RecvMsg inside
	// reconcile's GetAccount while the book streams kept flowing, so every liveness
	// signal (stream heartbeat, reconnectLoop) looked healthy while the bot silently
	// stopped trading with an open position.
	//
	// 10s (the original value) was still tripping on ordinary Finam degradation:
	// near-daily DeadlineExceeded persisted through 2026-08-03 with no stalled-engine
	// root cause behind it, just a slow broker. Raised to 30s on 2026-08-11 to ride out
	// those round-trip spikes, then lowered back to 10s on 2026-08-16 as a deliberate
	// choice by the operator, overriding that finding. DeadlineExceeded is already in
	// execengine's maybeDelivered class (brokers.go), so a placement cut off here
	// resolves the order's fate instead of assuming it never landed — but expect the
	// near-daily DeadlineExceeded noise from 08-03 to return; if it does, that's this
	// value, not a new regression.
	unaryRPCTimeout = 10 * time.Second
	// refreshMargin is how long before a token's real expiry we renew it. The
	// schedule is anchored to the server-reported expires_at (see nextRefreshDelay)
	// rather than a fixed interval, so a stalled or delayed auth cycle can't let the
	// live token outlive its replacement.
	refreshMargin = 3 * time.Minute
	// fallbackReauthInterval is used when the token's expiry can't be determined.
	// It stays well under the observed ~15m token TTL so the token is never served
	// past expiry even without expires_at guidance.
	fallbackReauthInterval = 5 * time.Minute
	// minRefreshDelay floors the computed delay so a near-expired or clock-skewed
	// token can't spin the monitor into a busy re-auth loop.
	minRefreshDelay = 30 * time.Second
	// authStuckReconnect bounds how long the auth cycle may keep failing on the SAME
	// connection before monitor gives up on it and dials a fresh one. gRPC's own
	// connectivity state machine only reacts to transport-level failure, so it never
	// catches this failure mode: the connection keeps answering every OTHER RPC
	// normally while only Auth stalls or gets throttled. Live incident 2026-08-20: one
	// connection failed Auth continuously for ~40 minutes (reconcile/margin/PlaceOrder
	// on that SAME connection kept getting fast, clean "Unauthenticated" responses the
	// whole time — proof the transport itself was fine) while a completely separate
	// connection to the same two Finam IPs had zero auth problems, and a brand-new
	// connection authenticated on its very first attempt right after. Waiting longer
	// only accumulates cost: the live token expires mid-wait and every OTHER RPC starts
	// failing too.
	authStuckReconnect = 3 * time.Minute
)

type Config struct {
	Secret    string `json:"secret"`
	AccountID string `json:"account_id"`
}

type Client struct {
	// grpcClient is conn B — the trading connection. ALL unary RPC (PlaceOrder/
	// CancelOrder, order Status polling, reconcile/GetAccount, usage-metrics,
	// margin polling) and the JWT auth cycle ride this connection.
	//
	// It can be REPLACED at runtime by reconnectTrading (see authStuckReconnect), so
	// every read goes through connMu — a bare field read would race the swap.
	grpcClient *grpcclient.Client
	connMu     sync.RWMutex
	// redialTrading dials a replacement trading connection. Defaults to dialBroker in
	// NewClient; overridable in tests so reconnectTrading can be exercised without a
	// real Finam endpoint.
	redialTrading func() (*grpcclient.Client, error)
	// authStuckOverrideNs overrides authStuckReconnect when > 0 — set via
	// SetAuthStuckReconnect. Test-only: lets an integration harness observe the
	// reconnect trigger in seconds instead of the real 3-minute wait.
	authStuckOverrideNs atomic.Int64
	// reconnects counts successful reconnectTrading swaps, for tests to observe that
	// the trigger actually fired end-to-end (ReconnectCount).
	reconnects atomic.Int64
	// streamClient is conn A — a SEPARATE HTTP/2 connection to the same endpoint,
	// carrying every server-streaming subscription (orderbook of both legs,
	// own-fills, order-states, the public trade tape). Splitting the streams onto
	// their own TCP socket / loopyWriter / flow-control state means an order-RPC
	// write burst on conn B can no longer starve the quote streams' WINDOW_UPDATEs
	// and freeze the book (the 2026-07-22 stale-Si incident). Auth is shared: both
	// connections send the same GetJWT() token — there is no second login.
	streamClient *grpcclient.Client
	config       Config
	jwt          string
	jwtReady     sync.RWMutex
	closed       atomic.Bool
	ready        chan struct{}
	signalReady  sync.Once
	stop         chan struct{}
	stopOnce     sync.Once
}

// defaultAPIAddr — боевой эндпоинт Finam Trade API (TLS).
const defaultAPIAddr = "api.finam.ru:443"

// EnvAddr — переменная окружения, перенаправляющая ВЕСЬ брокерский трафик на
// локальный брокер-симулятор (brokersim, plaintext). Только для отладки: пустое
// значение (дефолт) означает боевой эндпоинт с TLS.
const EnvAddr = "QUANTCORE_FINAM_ADDR"

// dialRetryBase and dialRetryMax bound NewClientRetry's backoff. The dial itself is
// budgeted at 5s per connection inside grpcclient (and NewClient opens TWO), so the
// interesting failure is a slow or unreachable broker at STARTUP — a narrow window,
// but one where every caller used to die outright. The cap is what decides how long
// after the network comes back a recorder starts recording again, so it stays small.
const (
	dialRetryBase = 1 * time.Second
	dialRetryMax  = 30 * time.Second
)

// NewClientRetry dials the broker like NewClient, but treats a failed dial as a
// TRANSIENT condition rather than a terminal one: it keeps retrying with capped
// exponential backoff until a dial succeeds or ctx is done. label names the caller in
// the log ("basis_ema", "price logging", ...).
//
// It exists because a dial is the one broker interaction with no retry anywhere behind
// it. Everything after startup recovers on its own — gRPC reconnects the transport,
// streamBook/reconnectLoop re-establish streams, unary calls fail on their deadline and
// get re-driven by the caller — but the initial dial had exactly one attempt, so a
// single slow TLS handshake killed a bot (mlog.Fatalf) or silently dropped a listener
// role for the entire life of the process. Shortening the dial budget only helps if
// something retries; this is that something.
//
// The retried unit is all of NewClient, so it also covers the JWT wait (authTimeout) —
// deliberate: an auth call cut off by the same broker degradation is exactly as transient
// as a slow handshake. The cost is that a genuinely WRONG secret no longer dies, it
// retries about once a minute forever (30s auth wait + the capped backoff), naming the
// reason on every attempt. That trade favours the transient case on purpose: a bad secret
// is a deploy-time mistake visible in the first log line either way, while a broker that
// is merely slow used to cost a whole trading session.
func NewClientRetry(ctx context.Context, config Config, label string) (*Client, error) {
	return newClientRetry(ctx, func() (*Client, error) { return NewClient(config) }, label, dialRetryBase, dialRetryMax)
}

// newClientRetry is NewClientRetry with the dial injected, so the retry cadence can be
// exercised without a broker.
func newClientRetry(ctx context.Context, dial func() (*Client, error), label string, base, max time.Duration) (*Client, error) {
	for attempt := 1; ; attempt++ {
		client, err := dial()
		if err == nil {
			if attempt > 1 {
				mlog.Printf("[%s] broker connection established on attempt %d", label, attempt)
			}
			return client, nil
		}
		delay := dialBackoff(attempt, base, max)
		mlog.Printf("[%s] connecting to the broker failed (attempt %d): %v — retrying in %v", label, attempt, err, delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, fmt.Errorf("connecting to the broker for %s: %w", label, ctx.Err())
		}
	}
}

// dialBackoff is base doubled per attempt, capped at max. The doubling is computed by
// repeated multiplication rather than a shift so a long outage (attempt > 63) can't
// overflow the duration into a negative delay.
func dialBackoff(attempt int, base, max time.Duration) time.Duration {
	d := base
	for i := 1; i < attempt; i++ {
		if d >= max {
			break
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

func NewClient(config Config) (*Client, error) {
	grpcClient, err := dialBroker()
	if err != nil {
		return nil, err
	}

	// conn A — the streaming connection. Dialed through the exact same path as the
	// trading connection (same endpoint, same TLS/creds, same service config), so
	// the two are indistinguishable to the server apart from being separate sockets
	// and the larger flow-control windows (streamDialOpts). Fail-fast: if conn A
	// can't be established we abort startup (and tear down the trading connection we
	// just opened) rather than silently degrading to a single shared connection — a
	// silent fallback would make the stream/trade isolation vanish unnoticed and
	// reintroduce the very freeze this split exists to prevent.
	streamClient, err := dialBroker(streamDialOpts()...)
	if err != nil {
		_ = grpcClient.Close()
		return nil, fmt.Errorf("dialing streaming connection: %w", err)
	}

	client := &Client{
		grpcClient:    grpcClient,
		streamClient:  streamClient,
		config:        config,
		ready:         make(chan struct{}),
		stop:          make(chan struct{}),
		redialTrading: func() (*grpcclient.Client, error) { return dialBroker() },
	}

	go client.monitor()

	select {
	case <-client.ready:
		return client, nil
	case <-time.After(authTimeout):
		_ = client.Close()
		return nil, fmt.Errorf("timeout waiting for authentication")
	}
}

// dialBroker выбирает эндпоинт: боевой TLS по умолчанию, либо локальный сим
// plaintext'ом, если задан QUANTCORE_FINAM_ADDR. Переключение громко логируется —
// молча уйти «в прод» или «в сим» нельзя.
func dialBroker(extraOpts ...grpc.DialOption) (*grpcclient.Client, error) {
	if addr := os.Getenv(EnvAddr); addr != "" {
		mlog.Printf("[finam] %s=%s — INSECURE debug mode: ALL broker traffic goes to the local simulator, NOT to Finam", EnvAddr, addr)
		return grpcclient.NewClientInsecure(addr, extraOpts...)
	}
	return grpcclient.NewClient(defaultAPIAddr, extraOpts...)
}

// streamDialOpts returns the extra dial options applied to conn A only. The larger
// HTTP/2 flow-control windows (16 MiB connection and stream, vs gRPC's 64 KiB /
// ~1 MiB defaults) give the quote streams enough server-side credit that a single
// delayed WINDOW_UPDATE can't stall them — belt-and-suspenders on top of the socket
// split. This is a deliberately self-contained, easily-revertable knob: returning
// nil here restores plain-window behaviour on the streaming connection.
//
// The keepalive params add a transport-level (HTTP/2 PING) liveness check, independent
// of whether the subscribed feed itself has anything to send — this is what lets
// heartbeatTimeout in orderbook.go tolerate a genuinely quiet market instead of
// mistaking it for a dead connection (see that comment for the 29.07 measured impact).
//
// ⚠️ grpc client keepalive was tried once before and REVERTED (commit 1650563,
// 2026-07-07): "Finam's gateway does not ACK client keepalive pings ... only produces
// spurious teardowns". Before reinstating it here, that claim was re-verified LIVE
// against production on 2026-07-30 (ztmp/kalive_real, real secret, real
// SubscribeOrderBook on LEGA@RTSX, GODEBUG=http2debug=2): 208 PING frames sent over a
// deliberate 50s idle stretch, all 208 ACKed, zero "failed to receive ACK" errors,
// stream still healthy afterward. Finam's gateway (`server: envoy` in the response
// headers) answers HTTP/2 PING today — whatever caused the 07-07 finding, it isn't
// reproducible now. If connections start dying with "keepalive ping failed to receive
// ACK" after this, that old finding was right after all and this must be reverted again.
func streamDialOpts() []grpc.DialOption {
	const streamWindowSize = 16 << 20 // 16 MiB
	return []grpc.DialOption{
		grpc.WithInitialConnWindowSize(streamWindowSize),
		grpc.WithInitialWindowSize(streamWindowSize),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                15 * time.Second, // ping after this much silence, data or not
			Timeout:             5 * time.Second,  // no PONG within this long ⇒ dead, fail the stream
			PermitWithoutStream: true,
		}),
	}
}

func (c *Client) GetConn(ctx context.Context) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	gc := c.grpcClient
	c.connMu.RUnlock()
	return gc.GetConn(ctx)
}

// GetStreamConn returns conn A — the connection dedicated to server-streaming
// subscriptions. Every long-lived stream (orderbook, own-fills, order-states,
// trade tape) must dial through this instead of GetConn so that order-RPC write
// bursts on the trading connection cannot back up the streams' flow-control.
func (c *Client) GetStreamConn(ctx context.Context) (*grpc.ClientConn, error) {
	return c.streamClient.GetConn(ctx)
}

func (c *Client) GetJWT() string {
	c.jwtReady.RLock()
	defer c.jwtReady.RUnlock()
	return c.jwt
}

func (c *Client) GetConfig() Config {
	return c.config
}

// Close tears down BOTH connections (trading and streaming) and stops the auth
// monitor. The trading connection's error is returned; a streaming-connection
// close error is logged but not allowed to mask it, since both must always be
// attempted regardless of which fails.
func (c *Client) Close() error {
	c.closed.Store(true)
	c.stopOnce.Do(func() { close(c.stop) })
	if err := c.streamClient.Close(); err != nil {
		mlog.Printf("[finam] closing streaming connection: %v", err)
	}
	c.connMu.RLock()
	gc := c.grpcClient
	c.connMu.RUnlock()
	return gc.Close()
}

// dial returns the shared connection together with a context carrying the auth
// token — the boilerplate every unary Finam RPC needs — and the cancel func that
// releases it. CALLERS MUST defer cancel().
//
// The deadline is applied HERE, not left to each call site, because it is the one
// place every unary RPC funnels through: an RPC that forgets it doesn't merely time
// out late, it parks the engine goroutine forever (see unaryRPCTimeout). It wraps the
// caller's ctx, so an earlier deadline upstream still wins, and it covers GetConn too —
// waiting for a connection to become ready is just as unbounded as waiting for a reply,
// and waitForReady in the service config means a dead connection queues calls instead
// of failing them fast.
func (c *Client) dial(ctx context.Context) (*grpc.ClientConn, context.Context, context.CancelFunc, error) {
	rctx, cancel := context.WithTimeout(ctx, unaryRPCTimeout)

	conn, err := c.GetConn(rctx)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	return conn, metadata.AppendToOutgoingContext(rctx, "Authorization", c.GetJWT()), cancel, nil
}

// monitor refreshes the JWT periodically. The blocking work (connect + Auth RPC) is
// done WITHOUT holding jwtReady so readers via GetJWT are never blocked behind a
// network round-trip or a stalled connection; the write lock is taken only to publish
// the new token. A single bounded context covers the connect + Auth RPC so a stalled
// connection fails fast and retries instead of hanging the monitor (and therefore
// every reader) forever.
func (c *Client) monitor() {
	firstAuth := true
	var failingSince time.Time

	for !c.closed.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), authTimeout)
		conn, err := c.GetConn(ctx)
		if err != nil {
			cancel()
			mlog.Printf("[finam-auth] connection unavailable: %v; retrying in %v", err, authRetryDelay)
			failingSince = c.handleAuthFailure(failingSince, time.Now())
			if c.sleep(authRetryDelay) {
				return
			}
			continue
		}

		authClient := auth.NewAuthServiceClient(conn)
		authResp, err := authClient.Auth(ctx, &auth.AuthRequest{Secret: c.config.Secret})
		if err != nil {
			cancel()
			mlog.Printf("[finam-auth] Auth failed: %v; retrying in %v", err, authRetryDelay)
			failingSince = c.handleAuthFailure(failingSince, time.Now())
			if c.sleep(authRetryDelay) {
				return
			}
			continue
		}
		failingSince = time.Time{}

		c.jwtReady.Lock()
		c.jwt = authResp.Token
		c.jwtReady.Unlock()

		if firstAuth {
			c.signalReady.Do(func() { close(c.ready) })
			firstAuth = false
		}

		delay := c.nextRefreshDelay(ctx, authClient, authResp.Token)
		cancel()

		if c.sleep(delay) {
			return
		}
	}
}

// nextRefreshDelay asks the server when the freshly issued token expires and returns
// how long to wait before renewing it, leaving refreshMargin of slack. Anchoring the
// schedule to the token's real expiry — rather than a fixed interval measured from the
// previous auth — keeps a stalled or delayed cycle from letting the live token expire
// before its replacement is published. Falls back to fallbackReauthInterval if the
// expiry can't be determined.
func (c *Client) nextRefreshDelay(ctx context.Context, authClient auth.AuthServiceClient, token string) time.Duration {
	det, err := authClient.TokenDetails(ctx, &auth.TokenDetailsRequest{Token: token})
	if err != nil {
		mlog.Printf("[finam-auth] TokenDetails failed: %v; refreshing in %v", err, fallbackReauthInterval)
		return fallbackReauthInterval
	}
	if det.ExpiresAt == nil {
		mlog.Printf("[finam-auth] token expiry unknown; refreshing in %v", fallbackReauthInterval)
		return fallbackReauthInterval
	}
	return refreshDelay(det.ExpiresAt.AsTime(), time.Now())
}

// refreshDelay returns how long to wait before renewing a token that expires at
// expiresAt, renewing refreshMargin ahead of expiry and never sooner than
// minRefreshDelay.
func refreshDelay(expiresAt, now time.Time) time.Duration {
	d := expiresAt.Sub(now) - refreshMargin
	if d < minRefreshDelay {
		return minRefreshDelay
	}
	return d
}

// handleAuthFailure tracks how long the auth cycle has been failing continuously on the
// CURRENT connection and, once that streak reaches the (possibly overridden — see
// SetAuthStuckReconnect) authStuckReconnect threshold, replaces it via reconnectTrading
// instead of retrying the same one again. It returns the (possibly reset) failingSince for
// the caller to carry into the next iteration. now is injected so the threshold can be
// tested without a real wait.
func (c *Client) handleAuthFailure(failingSince, now time.Time) time.Time {
	if failingSince.IsZero() {
		return now
	}
	if now.Sub(failingSince) >= c.authStuckThreshold() {
		c.reconnectTrading(fmt.Sprintf("auth failing continuously for %v", now.Sub(failingSince)))
		return now
	}
	return failingSince
}

// authStuckThreshold returns the override set by SetAuthStuckReconnect if one is active,
// otherwise the production default authStuckReconnect.
func (c *Client) authStuckThreshold() time.Duration {
	if d := c.authStuckOverrideNs.Load(); d > 0 {
		return time.Duration(d)
	}
	return authStuckReconnect
}

// SetAuthStuckReconnect overrides how long the auth cycle may fail continuously before
// monitor forces a fresh trading connection. Test-only: an integration harness can shrink
// this to observe the reconnect trigger in seconds instead of the real 3-minute wait; d<=0
// restores the production default.
func (c *Client) SetAuthStuckReconnect(d time.Duration) {
	c.authStuckOverrideNs.Store(int64(d))
}

// ReconnectCount reports how many times reconnectTrading has successfully replaced the
// trading connection. Test-only observability for the authStuckReconnect trigger.
func (c *Client) ReconnectCount() int64 { return c.reconnects.Load() }

// reconnectTrading replaces conn B with a freshly dialed connection. Only conn B is
// touched — conn A (streaming) never carries the auth cycle and has its own liveness
// handling (see streamDialOpts) — and only via redialTrading, so tests can exercise this
// without a real Finam endpoint. A dial failure is logged and left for the next failed
// auth attempt to retry; the OLD connection is only closed once the swap has succeeded,
// so a failed redial never leaves the client without ANY connection.
func (c *Client) reconnectTrading(reason string) {
	newConn, err := c.redialTrading()
	if err != nil {
		mlog.Printf("[finam-auth] forced reconnect (%s) failed to dial a replacement trading connection: %v", reason, err)
		return
	}

	c.connMu.Lock()
	old := c.grpcClient
	c.grpcClient = newConn
	c.connMu.Unlock()
	c.reconnects.Add(1)

	mlog.Printf("[finam-auth] forced reconnect: trading connection replaced (%s)", reason)
	if err := old.Close(); err != nil {
		mlog.Printf("[finam-auth] closing the old trading connection after forced reconnect: %v", err)
	}
}

// sleep waits for d or until Close is called; it returns true if the client was
// closed, so the caller can stop promptly instead of lingering in a bare sleep.
func (c *Client) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return false
	case <-c.stop:
		return true
	}
}
