package utils

import (
	"context"
	"fmt"
	"time"
)

// ThrottleCfg Throttle's configuration
type ThrottleCfg struct {
	Max, NPerSec int
}

// Throttle current limitor
type Throttle struct {
	*ThrottleCfg

	token      struct{}
	tokensChan chan struct{}
	stopChan   chan struct{}
}

// NewThrottleWithCtx create new Throttle
//
// 90x faster than `rate.NewLimiter`
func NewThrottleWithCtx(ctx context.Context, cfg *ThrottleCfg) (t *Throttle, err error) {
	if cfg.NPerSec <= 0 {
		return nil, fmt.Errorf("NPerSec should greater than 0")
	}
	if cfg.Max < cfg.NPerSec {
		return nil, fmt.Errorf("Max should greater than NPerSec")
	}

	t = &Throttle{
		ThrottleCfg: cfg,
		token:       struct{}{},
		stopChan:    make(chan struct{}),
	}
	t.tokensChan = make(chan struct{}, t.Max)
	go t.runWithCtx(ctx)
	return t, nil
}

// Allow check whether is allowed
func (t *Throttle) Allow() bool {
	select {
	case <-t.tokensChan:
		return true
	default:
		return false
	}
}

// runWithCtx start throttle with context
func (t *Throttle) runWithCtx(ctx context.Context) {
	defer Logger.Debug("throttle exit")

	for i := 0; i < t.NPerSec; i++ {
		t.tokensChan <- t.token
	}

	var nBatch float64 = 10
	nPerBatch := float64(t.NPerSec) / nBatch
	interval := time.Duration(1/nBatch*1000) * time.Millisecond

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
TOKEN_LOOP:
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		case <-t.stopChan:
			return
		}

		for i := float64(0); i < nPerBatch; i++ {
			select {
			case t.tokensChan <- t.token:
			default:
				continue TOKEN_LOOP
			}
		}
	}
}

// Close stop throttle
func (t *Throttle) Close() {
	close(t.stopChan)
}

// Stop stop throttle
//
// Deprecated: replaced by Close
func (t *Throttle) Stop() {
	t.Close()
}
