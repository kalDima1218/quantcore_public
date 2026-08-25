package finam

import (
	"QuantCore/grpcclient"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// silentBroker starts a gRPC server that ACCEPTS every call and never answers, holding
// the stream open until the client gives up. That is the exact shape of the 2026-07-29
// production hang — not a refused connection or a dropped socket, both of which surface
// as errors on their own, but a live connection whose request simply gets no reply.
// It returns the address to dial.
func silentBroker(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(grpc.UnknownServiceHandler(
		func(_ any, stream grpc.ServerStream) error {
			<-stream.Context().Done() // accept, then stay mute until the caller bails out
			return stream.Context().Err()
		}))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// TestUnaryRPCBoundedByDeadline pins the invariant that lets a stalled broker be survived:
// a unary RPC must fail on its own deadline rather than block its caller forever. The
// caller here is the single engine goroutine — everything the bot does, from placing an
// order to reconciling, runs on it, so one unbounded RPC stops the whole strategy while
// every health signal keeps reading green.
func TestUnaryRPCBoundedByDeadline(t *testing.T) {
	gc, err := grpcclient.NewClientInsecure(silentBroker(t))
	if err != nil {
		t.Fatalf("dialing the silent broker: %v", err)
	}
	t.Cleanup(func() { _ = gc.Close() })

	client := &Client{grpcClient: gc, config: Config{AccountID: "test-account"}}

	// Slack covers dial + scheduling, not a second RPC attempt: the point is that the
	// call returns on its OWN deadline, so the budget is the deadline plus a little.
	const slack = 5 * time.Second

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := GetAccount(client)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("GetAccount returned nil error from a broker that never answered")
		}
		if elapsed := time.Since(start); elapsed > unaryRPCTimeout+slack {
			t.Fatalf("GetAccount returned after %v, want it bounded by unaryRPCTimeout (%v)", elapsed, unaryRPCTimeout)
		}
	case <-time.After(unaryRPCTimeout + slack):
		t.Fatalf("GetAccount still blocked after %v: a unary RPC against a broker that "+
			"accepted the request and went silent must fail on its deadline, not park the "+
			"engine goroutine forever (prod hang 2026-07-29: 44 minutes inside reconcile)",
			unaryRPCTimeout+slack)
	}
}

func TestRefreshDelay(t *testing.T) {
	now := time.Date(2026, 7, 2, 20, 0, 0, 0, time.UTC)

	t.Run("renews refreshMargin ahead of expiry", func(t *testing.T) {
		// 15m token, typical Finam TTL.
		got := refreshDelay(now.Add(15*time.Minute), now)
		if want := 15*time.Minute - refreshMargin; got != want {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("near-expiry token floors at minRefreshDelay", func(t *testing.T) {
		if got := refreshDelay(now.Add(1*time.Minute), now); got != minRefreshDelay {
			t.Fatalf("want %v, got %v", minRefreshDelay, got)
		}
	})

	t.Run("already-expired token floors at minRefreshDelay", func(t *testing.T) {
		if got := refreshDelay(now.Add(-5*time.Minute), now); got != minRefreshDelay {
			t.Fatalf("want %v, got %v", minRefreshDelay, got)
		}
	})
}

// A failed dial must be treated as transient, not terminal: newClientRetry keeps trying
// until one succeeds. The bug this pins is the startup asymmetry — nothing in the tree
// retries a dial, so a single slow handshake (5s budget, see grpcclient.newClient) used
// to kill a bot outright and silently drop a listener role for the process's whole life.
func TestNewClientRetryKeepsDialingUntilSuccess(t *testing.T) {
	want := &Client{}
	attempts := 0
	dial := func() (*Client, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("context deadline exceeded")
		}
		return want, nil
	}

	got, err := newClientRetry(context.Background(), dial, "test", time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatalf("newClientRetry: %v", err)
	}
	if got != want {
		t.Fatalf("got %p, want the client from the successful dial %p", got, want)
	}
	if attempts != 3 {
		t.Fatalf("dialed %d times, want 3 (two failures then a success)", attempts)
	}
}

// Retrying forever must still answer to shutdown: a Ctrl-C while the broker is
// unreachable has to return, not park the caller in the backoff loop.
func TestNewClientRetryStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dial := func() (*Client, error) { return nil, errors.New("connection refused") }

	go func() { time.Sleep(10 * time.Millisecond); cancel() }()

	done := make(chan error, 1)
	go func() {
		_, err := newClientRetry(ctx, dial, "test", 5*time.Millisecond, 5*time.Millisecond)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want a context.Canceled-wrapping error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("newClientRetry ignored a cancelled context and kept retrying")
	}
}

// TestHandleAuthFailureReconnectsAfterSustainedFailure pins the fix for the 2026-08-20
// incident: one connection failed Auth continuously for ~40 minutes while a completely
// separate connection to the same Finam endpoint had zero problems, and reconcile/margin/
// PlaceOrder on the SAME stuck connection kept getting fast, clean "Unauthenticated"
// responses the whole time — proof gRPC's own connectivity state machine had no reason to
// ever mark that connection unhealthy (it correctly saw the transport working). Nothing
// there will ever force a reconnect, so monitor must do it itself once a failure streak on
// one connection has run long enough to be worth abandoning rather than retried again.
func TestHandleAuthFailureReconnectsAfterSustainedFailure(t *testing.T) {
	oldConn, err := grpcclient.NewClientInsecure(silentBroker(t))
	if err != nil {
		t.Fatalf("dialing the old connection: %v", err)
	}
	newConn, err := grpcclient.NewClientInsecure(silentBroker(t))
	if err != nil {
		t.Fatalf("dialing the replacement connection: %v", err)
	}
	t.Cleanup(func() { _ = newConn.Close() })

	reconnects := 0
	c := &Client{
		grpcClient: oldConn,
		redialTrading: func() (*grpcclient.Client, error) {
			reconnects++
			return newConn, nil
		},
	}

	base := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	failingSince := c.handleAuthFailure(time.Time{}, base)
	if !failingSince.Equal(base) {
		t.Fatalf("the first failure must record its own time as the start of the streak, got %v", failingSince)
	}
	if reconnects != 0 {
		t.Fatalf("must not reconnect on the very first failure, got %d reconnects", reconnects)
	}

	failingSince = c.handleAuthFailure(failingSince, base.Add(authStuckReconnect-time.Second))
	if reconnects != 0 {
		t.Fatalf("must not reconnect before authStuckReconnect has elapsed, got %d reconnects", reconnects)
	}
	if c.grpcClient != oldConn {
		t.Fatal("must not swap the connection before the threshold is crossed")
	}

	failingSince = c.handleAuthFailure(failingSince, base.Add(authStuckReconnect))
	if reconnects != 1 {
		t.Fatalf("want exactly 1 reconnect once the streak crosses authStuckReconnect, got %d", reconnects)
	}
	if c.grpcClient != newConn {
		t.Fatal("grpcClient must be swapped to the freshly redialed connection")
	}
	if !failingSince.Equal(base.Add(authStuckReconnect)) {
		t.Fatalf("failingSince must reset to the reconnect moment, got %v", failingSince)
	}

	// A further failure right after must not reconnect again immediately — the clock
	// restarted at the reconnect moment, not at zero.
	c.handleAuthFailure(failingSince, base.Add(authStuckReconnect+time.Second))
	if reconnects != 1 {
		t.Fatalf("must not reconnect again 1s after the previous reconnect, got %d reconnects", reconnects)
	}
}

// TestHandleAuthFailureDoesNotReconnectWhenTheDialFails pins that a failed redial leaves
// the client on its existing (if stuck) connection rather than with none at all — the next
// failed auth attempt gets another chance to reconnect.
func TestHandleAuthFailureDoesNotReconnectWhenTheDialFails(t *testing.T) {
	oldConn, err := grpcclient.NewClientInsecure(silentBroker(t))
	if err != nil {
		t.Fatalf("dialing the old connection: %v", err)
	}
	t.Cleanup(func() { _ = oldConn.Close() })

	dialAttempts := 0
	c := &Client{
		grpcClient: oldConn,
		redialTrading: func() (*grpcclient.Client, error) {
			dialAttempts++
			return nil, errors.New("dial failed")
		},
	}

	base := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	failingSince := c.handleAuthFailure(time.Time{}, base)
	failingSince = c.handleAuthFailure(failingSince, base.Add(authStuckReconnect))

	if dialAttempts != 1 {
		t.Fatalf("want exactly 1 redial attempt, got %d", dialAttempts)
	}
	if c.grpcClient != oldConn {
		t.Fatal("a failed redial must leave the existing connection in place")
	}
	if !failingSince.Equal(base.Add(authStuckReconnect)) {
		t.Fatalf("failingSince must still advance so the next failure gets another chance to reconnect, got %v", failingSince)
	}
}

// TestSetAuthStuckReconnectOverridesTheThreshold pins the test-only override that lets an
// integration harness observe the reconnect trigger in seconds instead of the real
// 3-minute production wait.
func TestSetAuthStuckReconnectOverridesTheThreshold(t *testing.T) {
	c := &Client{}

	if got := c.authStuckThreshold(); got != authStuckReconnect {
		t.Fatalf("with no override, want the production default %v, got %v", authStuckReconnect, got)
	}

	c.SetAuthStuckReconnect(5 * time.Second)
	if got := c.authStuckThreshold(); got != 5*time.Second {
		t.Fatalf("want the override 5s, got %v", got)
	}

	c.SetAuthStuckReconnect(0)
	if got := c.authStuckThreshold(); got != authStuckReconnect {
		t.Fatalf("d<=0 must restore the production default %v, got %v", authStuckReconnect, got)
	}
}

// The backoff must grow (so a broker outage isn't hammered) and stop growing at the cap
// (so a recovered network is picked up within a bounded delay, not hours later).
func TestDialBackoffDoublesUpToCap(t *testing.T) {
	const base, max = time.Second, 8 * time.Second
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second, 8 * time.Second}
	for i, w := range want {
		if got := dialBackoff(i+1, base, max); got != w {
			t.Fatalf("attempt %d: got %v, want %v", i+1, got, w)
		}
	}
}
