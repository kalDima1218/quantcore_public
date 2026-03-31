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
	"google.golang.org/grpc/keepalive"
)

var (
	ErrNotConnected = errors.New("not connected")
	ErrClosed       = errors.New("client closed")
)

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

func NewClient(addr string) (*Client, error) {
	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: false,
	})

	kaParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithDefaultServiceConfig(`{
            "methodConfig": [{
                "name": [{"service": ""}],
                "waitForReady": true
            }]
        }`),
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = client.waitForState(ctx, connectivity.Ready)
	cancel()

	if err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
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

func (c *Client) MustGetConn() *grpc.ClientConn {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := c.GetConn(ctx)
	if err != nil {
		panic("grpc connection not ready: " + err.Error())
	}
	return conn
}

func (c *Client) IsReady() bool {
	return c.ready.Load()
}

func (c *Client) WaitReady(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := c.GetConn(ctx)
	return err
}

func (c *Client) Close() error {
	c.closed.Store(true)
	c.closeOnce.Do(func() { close(c.closedCh) })
	c.setReady(false)
	return c.conn.Close()
}
