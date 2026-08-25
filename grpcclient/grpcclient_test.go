package grpcclient

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// newTestClient builds a Client in the same state NewClient leaves it in right after a
// successful dial (ready channels initialized, ready=true, a real but never-connected
// *grpc.ClientConn so Close() is safe to call) — WITHOUT a running monitor() goroutine, so
// tests can drive setReady/Close/GetConn deterministically and race-test them without a
// live server. grpc.NewClient does not dial eagerly, so no network I/O happens here.
func newTestClient(t *testing.T) *Client {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	c := &Client{
		conn:     conn,
		readyCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}
	c.setReady(true)
	return c
}

// A closed client must never become ready again, however late a stale "still Ready"
// connectivity report (monitor() having read the wire moments before Close() ran) arrives.
// This is the race from grpcclient.go review: monitor reads Ready before Close begins, then
// calls setReady(true) after Close has already set closed=true — without a guard, that
// resurrects ready=true and reopens readyCh on an otherwise-closed client.
func TestSetReadyTrueIsNoOpAfterClosed(t *testing.T) {
	c := newTestClient(t)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.ready.Load() {
		t.Fatal("setup: Close must leave ready=false")
	}

	c.setReady(true) // the delayed, stale "still Ready" report racing in after Close

	if c.ready.Load() {
		t.Fatal("a closed client must never report ready again")
	}
	select {
	case <-c.readyCh:
		// readyCh being closed would wake every GetConn caller blocked in select as if
		// the connection just became ready.
		t.Fatal("readyCh must not be (re)closed on a closed client")
	default:
	}
}

// GetConn must check closed before ready: Close() sets closed=true, THEN calls
// setReady(false) — there is a real window where ready is still true but closed already
// is. The old check order (ready first) would hand back a connection that is being torn
// down instead of ErrClosed.
func TestGetConnChecksClosedBeforeReadyFastPath(t *testing.T) {
	c := newTestClient(t)
	c.closed.Store(true) // simulate exactly Close()'s first action, before its own setReady(false)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.GetConn(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("GetConn = %v, want ErrClosed (ready=true but closed=true must not win)", err)
	}
}

// Concurrency stress: one goroutine cycles Ready<->NotReady (as monitor() does across
// reconnects, including racing setReady(true) calls against the closing transition) while
// many goroutines call GetConn. Run with -race. A GetConn call genuinely concurrent with
// the not-yet-finished close is allowed to see either answer — the invariant under test is
// that the client SETTLES: once closed=true has been observed and the flicker stopped, ready
// stays false and every subsequent GetConn reliably reports ErrClosed, with no lingering
// resurrection from a delayed setReady(true).
func TestGetConnSettlesClosedUnderConcurrentReadyFlicker(t *testing.T) {
	c := newTestClient(t)

	stopFlicker := make(chan struct{})
	var flickerWG sync.WaitGroup
	flickerWG.Add(1)
	go func() {
		defer flickerWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stopFlicker:
				return
			default:
			}
			c.setReady(i%2 == 0) // keeps calling setReady(true) even after closed flips
		}
	}()

	var callersWG sync.WaitGroup
	stopCallers := make(chan struct{})
	for i := 0; i < 8; i++ {
		callersWG.Add(1)
		go func() {
			defer callersWG.Done()
			for {
				select {
				case <-stopCallers:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
				c.GetConn(ctx) // discarded: concurrent with an in-progress close, either answer is valid here
				cancel()
			}
		}()
	}

	time.Sleep(5 * time.Millisecond)
	c.closed.Store(true) // mirrors Close()'s own action order: closed flips before setReady(false)
	c.setReady(false)

	// Give the flicker goroutine's guarded setReady(true) calls a chance to keep racing in
	// for a while longer, all of which must now be no-ops.
	time.Sleep(5 * time.Millisecond)
	close(stopFlicker)
	flickerWG.Wait()
	close(stopCallers)
	callersWG.Wait()

	if c.ready.Load() {
		t.Fatal("ready did not settle to false: a racing setReady(true) resurrected it after close")
	}
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err := c.GetConn(ctx)
		cancel()
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("GetConn after settling = %v, want ErrClosed", err)
		}
	}
}
