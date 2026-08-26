package finambroker

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"QuantCore/strategies/execengine2"
)

type fakeAPI struct {
	place  func(context.Context, execengine2.OrderRequest, string) (apiOrder, error)
	find   func(context.Context, string) (apiOrder, bool, error)
	cancel func(context.Context, string) (apiOrder, error)
	status func(context.Context, string) (apiOrder, error)
}

func (f *fakeAPI) Place(
	ctx context.Context,
	req execengine2.OrderRequest,
	clientID string,
) (apiOrder, error) {
	return f.place(ctx, req, clientID)
}

func (f *fakeAPI) Find(ctx context.Context, clientID string) (apiOrder, bool, error) {
	if f.find == nil {
		return apiOrder{}, false, nil
	}
	return f.find(ctx, clientID)
}

func (f *fakeAPI) Cancel(ctx context.Context, orderID string) (apiOrder, error) {
	return f.cancel(ctx, orderID)
}

func (f *fakeAPI) Status(ctx context.Context, orderID string) (apiOrder, error) {
	return f.status(ctx, orderID)
}

type waitFunc func(context.Context, time.Duration) error

func (f waitFunc) Wait(ctx context.Context, d time.Duration) error { return f(ctx, d) }

func fastWait(ctx context.Context, _ time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type testLimit struct {
	allow bool
	calls int
	kind  execengine2.LimitKind
}

func (l *testLimit) Take(_ int64, kind execengine2.LimitKind) bool {
	l.calls++
	l.kind = kind
	return l.allow
}

func (*testLimit) Remaining() int64 { return 100 }

func testIDs(t *testing.T) *clientIDs {
	t.Helper()
	ids, err := newClientIDs(bytes.NewReader(make([]byte, idRandomBytes)))
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func limitBuy() execengine2.OrderRequest {
	return execengine2.OrderRequest{
		Symbol: "SBER", Side: execengine2.SideBuy, Kind: execengine2.OrderLimit,
		Lots: 2, Price: 100,
	}
}

func TestClientIDsHaveFixedSize(t *testing.T) {
	ids := testIDs(t)
	first, err := ids.Next()
	if err != nil {
		t.Fatal(err)
	}
	second, err := ids.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 20 || len(second) != 20 {
		t.Fatalf("id sizes = %d and %d, want 20", len(first), len(second))
	}
	if first == second {
		t.Fatal("two calls returned the same id")
	}

	ids.count.Store(maxIDCount - 1)
	last, err := ids.Next()
	if err != nil || len(last) != 20 {
		t.Fatalf("last id = %q, err = %v", last, err)
	}
	if _, err := ids.Next(); err == nil {
		t.Fatal("id counter must stop before the value grows past 20 chars")
	}
}

func TestGatewayFindsOrderAfterLostReply(t *testing.T) {
	req := limitBuy()
	var sentID string
	api := &fakeAPI{}
	api.place = func(_ context.Context, _ execengine2.OrderRequest, clientID string) (apiOrder, error) {
		sentID = clientID
		return apiOrder{}, status.Error(codes.Unavailable, "lost reply")
	}
	api.find = func(_ context.Context, clientID string) (apiOrder, bool, error) {
		if clientID != sentID {
			t.Fatalf("find id = %q, sent id = %q", clientID, sentID)
		}
		return apiOrder{
			id: "order-1", symbol: req.Symbol, side: req.Side, kind: req.Kind,
			lots: req.Lots, price: req.Price,
		}, true, nil
	}
	g := &Gateway{api: api, ids: testIDs(t), waiter: waitFunc(fastWait)}

	orderID, err := g.Place(context.Background(), req)
	if err != nil || orderID != "order-1" {
		t.Fatalf("order id = %q, err = %v", orderID, err)
	}
}

func TestGatewayUsesSameClientIDOnRetry(t *testing.T) {
	req := limitBuy()
	var ids []string
	limit := &testLimit{allow: true}
	api := &fakeAPI{}
	api.place = func(_ context.Context, _ execengine2.OrderRequest, clientID string) (apiOrder, error) {
		ids = append(ids, clientID)
		if len(ids) == 1 {
			return apiOrder{}, status.Error(codes.Unavailable, "lost reply")
		}
		return apiOrder{
			id: "order-2", symbol: req.Symbol, side: req.Side, kind: req.Kind,
			lots: req.Lots, price: req.Price,
		}, nil
	}
	g := &Gateway{api: api, ids: testIDs(t), waiter: waitFunc(fastWait), limit: limit}

	orderID, err := g.Place(context.Background(), req)
	if err != nil || orderID != "order-2" {
		t.Fatalf("order id = %q, err = %v", orderID, err)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("retry ids = %v, want the same id", ids)
	}
	if limit.calls != 1 || limit.kind != execengine2.LimitNormal {
		t.Fatalf("retry limit calls = %d, kind = %d", limit.calls, limit.kind)
	}
}

func TestGatewayDoesNotRetryWhenLimitBlocks(t *testing.T) {
	req := limitBuy()
	placeCalls := 0
	limit := &testLimit{allow: false}
	api := &fakeAPI{
		place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
			placeCalls++
			return apiOrder{}, status.Error(codes.Unavailable, "lost reply")
		},
	}
	g := &Gateway{api: api, ids: testIDs(t), waiter: waitFunc(fastWait), limit: limit}

	_, err := g.Place(context.Background(), req)
	if err == nil || !execengine2.OrderMayExist(err) {
		t.Fatalf("err = %v, want unknown order", err)
	}
	if placeCalls != 1 || limit.calls != 1 {
		t.Fatalf("place calls = %d, retry limit calls = %d", placeCalls, limit.calls)
	}
}

func TestGatewayStopsWhenContextIsCanceled(t *testing.T) {
	req := limitBuy()
	ctx, cancel := context.WithCancel(context.Background())
	placeCalls := 0
	findCalls := 0
	api := &fakeAPI{}
	api.place = func(got context.Context, _ execengine2.OrderRequest, _ string) (apiOrder, error) {
		if got != ctx {
			t.Fatal("gateway replaced the caller context")
		}
		placeCalls++
		cancel()
		return apiOrder{}, status.Error(codes.Unavailable, "lost reply")
	}
	api.find = func(context.Context, string) (apiOrder, bool, error) {
		findCalls++
		return apiOrder{}, false, nil
	}
	g := &Gateway{api: api, ids: testIDs(t), waiter: waitFunc(fastWait)}

	_, err := g.Place(ctx, req)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if !execengine2.OrderMayExist(err) {
		t.Fatal("canceled send must stay unknown")
	}
	if _, ok := execengine2.ErrorClientID(err); !ok {
		t.Fatal("unknown send lost its client id")
	}
	if placeCalls != 1 || findCalls != 0 {
		t.Fatalf("place calls = %d, find calls = %d", placeCalls, findCalls)
	}
}

func TestGatewayMarksClearRejectAsNotPlaced(t *testing.T) {
	findCalls := 0
	api := &fakeAPI{
		place: func(context.Context, execengine2.OrderRequest, string) (apiOrder, error) {
			return apiOrder{}, status.Error(codes.InvalidArgument, "bad lots")
		},
		find: func(context.Context, string) (apiOrder, bool, error) {
			findCalls++
			return apiOrder{}, false, nil
		},
	}
	g := &Gateway{api: api, ids: testIDs(t), waiter: waitFunc(fastWait)}

	_, err := g.Place(context.Background(), limitBuy())
	if err == nil || execengine2.OrderMayExist(err) {
		t.Fatalf("err = %v, want a clear not-placed error", err)
	}
	if findCalls != 0 {
		t.Fatalf("find calls = %d, want 0", findCalls)
	}
}

func TestGatewayPassesContextToCancelAndStatus(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "ok")
	api := &fakeAPI{}
	api.cancel = func(got context.Context, orderID string) (apiOrder, error) {
		if got.Value(key{}) != "ok" || orderID != "order-3" {
			t.Fatal("bad cancel input")
		}
		return apiOrder{filled: 2}, nil
	}
	api.status = func(got context.Context, orderID string) (apiOrder, error) {
		if got.Value(key{}) != "ok" || orderID != "order-3" {
			t.Fatal("bad status input")
		}
		return apiOrder{filled: 3, done: true}, nil
	}
	g := &Gateway{api: api, ids: testIDs(t), waiter: waitFunc(fastWait)}

	cancelResult, err := g.Cancel(ctx, "order-3")
	if err != nil || cancelResult.Filled != 2 {
		t.Fatalf("cancel = %+v, err = %v", cancelResult, err)
	}
	orderStatus, err := g.Status(ctx, "order-3")
	if err != nil || orderStatus.Filled != 3 || !orderStatus.Done {
		t.Fatalf("status = %+v, err = %v", orderStatus, err)
	}
}
