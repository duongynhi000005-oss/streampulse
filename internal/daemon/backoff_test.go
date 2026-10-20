// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package daemon

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePinger records Ping invocations and delegates to pingErr.
type fakePinger struct {
	mu      sync.Mutex
	pings   int
	pingErr func() error
}

func (f *fakePinger) Ping(ctx context.Context) error {
	f.mu.Lock()
	f.pings++
	fn := f.pingErr
	f.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return nil
}

func (f *fakePinger) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pings
}

func TestExponentialBackoffSequence(t *testing.T) {
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for i, w := range want {
		assert.Equal(t, w, exponentialBackoff(i), "backoff(%d)", i)
	}
}

func TestEnsureConnectedRetriesWithBackoff(t *testing.T) {
	fails := 0
	pinger := &fakePinger{pingErr: func() error {
		fails++
		if fails <= 2 {
			return errors.New("dial: connection refused")
		}
		return nil
	}}
	d := &Daemon{
		client:    pinger,
		backoffFn: func(int) time.Duration { return 10 * time.Millisecond },
	}

	require.NoError(t, d.ensureConnected(context.Background()))
	assert.Equal(t, 3, pinger.count(), "ping must be retried with backoff until it succeeds")
}

func TestEnsureConnectedSkipsWhenBrokerUp(t *testing.T) {
	pinger := &fakePinger{}
	d := &Daemon{
		client:    pinger,
		backoffFn: func(int) time.Duration { return 10 * time.Millisecond },
	}

	require.NoError(t, d.ensureConnected(context.Background()))
	assert.Equal(t, 1, pinger.count(), "healthy broker is pinged exactly once")
}

func TestEnsureConnectedAbortsWaitOnCancel(t *testing.T) {
	pinger := &fakePinger{pingErr: func() error { return errors.New("down") }}
	d := &Daemon{
		client:    pinger,
		backoffFn: func(int) time.Duration { return time.Hour }, // never fires within the test
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := d.ensureConnected(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second, "must not wait out the backoff on cancel")
	assert.Equal(t, 1, pinger.count())
}

// TestScrapeProceedsAfterBrokerRecovers runs the full loop: pings fail at
// first, then recover, and the scrape must proceed.
func TestScrapeProceedsAfterBrokerRecovers(t *testing.T) {
	fails := 0
	pinger := &fakePinger{pingErr: func() error {
		fails++
		if fails <= 2 {
			return errors.New("down")
		}
		return nil
	}}
	scraper := &fakeScraper{}
	d := &Daemon{
		client:    pinger,
		scraper:   scraper,
		backoffFn: func(int) time.Duration { return 10 * time.Millisecond },
		stopped:   make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tick := make(chan time.Time)
	d.goLoop(func() { runScrapeLoop(ctx, d, tick) })
	tick <- time.Now()

	require.Eventually(t, func() bool {
		return scraper.count() == 1 && !d.scraping.Load()
	}, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 3, pinger.count())
}

// TestDaemonRunsWhileBrokerDown verifies the daemon keeps serving /metrics
// while the broker is unreachable and exits cleanly on shutdown.
func TestDaemonRunsWhileBrokerDown(t *testing.T) {
	pinger := &fakePinger{pingErr: func() error { return errors.New("down") }}
	store := &fakeStore{}
	stats := NewScrapeStats()
	prom := NewPromServer(&config.PromConfig{Listen: "127.0.0.1:0", Path: "/metrics"},
		stats, PromOptions{Version: "test"})

	d := &Daemon{
		cfg:       config.DefaultConfig(),
		store:     store,
		client:    pinger,
		scraper:   &fakeScraper{},
		stats:     stats,
		prom:      prom,
		backoffFn: func(int) time.Duration { return 20 * time.Millisecond },
		stopped:   make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	// The metrics endpoint must stay up while the broker is down.
	require.Eventually(t, func() bool {
		addr := prom.Addr()
		if addr == nil {
			return false
		}
		resp, err := http.Get("http://" + addr.String() + "/metrics")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 3*time.Second, 20*time.Millisecond)

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after cancel")
	}
}
