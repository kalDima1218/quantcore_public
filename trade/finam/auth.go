package finam

import (
	"QuantCore/grpcclient"
	"context"
	"encoding/json"
	"fmt"
	"github.com/FinamWeb/finam-trade-api/go/grpc/tradeapi/v1/auth"
	"google.golang.org/grpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Secret    string `json:"secret"`
	AccountID string `json:"account_id"`
}

type Client struct {
	grpcClient  *grpcclient.Client
	config      Config
	jwt         string
	jwtReady    sync.RWMutex
	closed      atomic.Bool
	ready       chan struct{}
	signalReady sync.Once
}

func NewClient(config Config) (*Client, error) {
	grpcClient, err := grpcclient.NewClient("api.finam.ru:443")
	if err != nil {
		return nil, err
	}

	client := &Client{
		grpcClient: grpcClient,
		config:     config,
		ready:      make(chan struct{}),
	}
	client.closed.Store(false)

	go client.monitor()

	select {
	case <-client.ready:
		return client, nil
	case <-time.After(30 * time.Second):
		_ = client.Close()
		return nil, fmt.Errorf("timeout waiting for authentication")
	}
}

func (c *Client) GetConn(ctx context.Context) (*grpc.ClientConn, error) {
	return c.grpcClient.GetConn(ctx)
}

func (c *Client) GetJWT() string {
	c.jwtReady.RLock()
	defer c.jwtReady.RUnlock()
	return c.jwt
}

func (c *Client) GetConfig() Config {
	return c.config
}

func (c *Client) Close() error {
	c.closed.Store(true)
	return c.grpcClient.Close()
}

func (c *Client) monitor() {
	const initialAuthRetryDelay = 1 * time.Second
	firstAuth := true

	for !c.closed.Load() {
		c.jwtReady.Lock()

		conn, err := c.GetConn(context.Background())
		if err != nil {
			c.jwtReady.Unlock()
			time.Sleep(initialAuthRetryDelay)
			continue
		}

		authClient := auth.NewAuthServiceClient(conn)

		authResp, err := authClient.Auth(
			context.Background(),
			&auth.AuthRequest{Secret: c.config.Secret},
		)

		if err != nil {
			c.jwtReady.Unlock()
			time.Sleep(initialAuthRetryDelay)
			continue
		}

		c.jwt = authResp.Token

		c.jwtReady.Unlock()

		if firstAuth {
			c.signalReady.Do(func() {
				close(c.ready)
			})
			firstAuth = false
		}

		time.Sleep(10 * time.Minute)
	}
}

func ReadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filename, err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return &config, nil
}
