package grpcclient

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrClosed = errors.New("client closed")

type Client struct {
	conn *grpc.ClientConn
	addr string
	opts []grpc.DialOption

	ready  atomic.Bool
	closed atomic.Bool

	readyCh   chan struct{}
	closedCh  chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
}

// NewClient dials addr over TLS. extraOpts are appended after the base dial
// options, letting a caller tune a specific connection (e.g. larger HTTP/2 flow-
// control windows for a stream-only connection) without affecting others; passing
// none reproduces the previous behaviour exactly.
func NewClient(addr string, extraOpts ...grpc.DialOption) (*Client, error) {
	return newClient(addr, credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: false,
	}), extraOpts...)
}

// NewClientInsecure dials addr WITHOUT TLS. It exists solely for pointing the
// bot at a local broker simulator (brokersim) on loopback; production Finam
// endpoints must go through NewClient. extraOpts behave as in NewClient.
func NewClientInsecure(addr string, extraOpts ...grpc.DialOption) (*Client, error) {
	return newClient(addr, insecure.NewCredentials(), extraOpts...)
}

func newClient(addr string, creds credentials.TransportCredentials, extraOpts ...grpc.DialOption) (*Client, error) {
	// No gRPC client keepalive: Finam's gateway does not ACK client keepalive
	// pings, so they only surface as spurious "keepalive ping failed to receive
	// ACK" errors that tear down otherwise-fine connections. Finam closes
	// idle/expired connections on its own; a server-initiated close makes the
	// active stream's Recv() return an error, which already drives reconnection.
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(`{
            "methodConfig": [{
                "name": [{"service": ""}],
                "waitForReady": true
            }]
        }`),
	}
	opts = append(opts, extraOpts...)

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:     conn,
		addr:     addr,
		opts:     opts,
		readyCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}

	conn.Connect()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = client.waitForState(ctx, connectivity.Ready)
	cancel()

	if err != nil {
		errConn := conn.Close()
		if errConn != nil {
			return nil, errConn
		}
		return nil, err
	}

	client.setReady(true)

	go client.monitor()

	return client, nil
}

func (c *Client) waitForState(ctx context.Context, target connectivity.State) error {
	for {
		state := c.conn.GetState()
		if state == target {
			return nil
		}
		if state == connectivity.Shutdown {
			return ErrClosed
		}
		if !c.conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func (c *Client) setReady(ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	wasReady := c.ready.Load()

	if ready && !wasReady {
		c.ready.Store(true)
		close(c.readyCh)
	} else if !ready && wasReady {
		c.ready.Store(false)
		c.readyCh = make(chan struct{})
	}
}

func (c *Client) getReadyCh() chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readyCh
}

func (c *Client) monitor() {
	for !c.closed.Load() {
		state := c.conn.GetState()

		switch state {
		case connectivity.Ready:
			c.setReady(true)

		case connectivity.Idle:
			c.setReady(false)
			c.conn.Connect()

		case connectivity.Connecting, connectivity.TransientFailure:
			c.setReady(false)

		case connectivity.Shutdown:
			c.setReady(false)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c.conn.WaitForStateChange(ctx, state)
		cancel()
	}
}

func (c *Client) GetConn(ctx context.Context) (*grpc.ClientConn, error) {
	if c.ready.Load() {
		return c.conn, nil
	}

	if c.closed.Load() {
		return nil, ErrClosed
	}

	select {
	case <-c.getReadyCh():
		if c.closed.Load() {
			return nil, ErrClosed
		}
		return c.conn, nil
	case <-c.closedCh:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) Close() error {
	c.closed.Store(true)
	c.closeOnce.Do(func() { close(c.closedCh) })
	c.setReady(false)
	return c.conn.Close()
}
