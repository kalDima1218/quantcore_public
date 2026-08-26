package budget

import (
	"context"
	"errors"
	"time"
)

// Controller сбрасывает лимит по таймеру в отдельной горутине.
// Сам Atomic ничего не знает о времени.
type Controller struct {
	budget *Atomic
	limit  int64
	window time.Duration
}

// NewController создаёт цикл сброса с постоянным окном.
func NewController(b *Atomic, limit int64, window time.Duration) (*Controller, error) {
	if b == nil {
		return nil, errors.New("budget is required")
	}
	if limit < 0 {
		return nil, errors.New("budget limit must not be negative")
	}
	if window <= 0 {
		return nil, errors.New("budget window must be positive")
	}
	return &Controller{budget: b, limit: limit, window: window}, nil
}

// Run сбрасывает счётчик, пока не закрыт ctx.
func (c *Controller) Run(ctx context.Context) {
	ticker := time.NewTicker(c.window)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.budget.Reset(c.limit)
		}
	}
}
