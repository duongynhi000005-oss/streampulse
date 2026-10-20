// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package daemon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeScraper records Collect invocations; a non-nil gate channel holds
// calls open so tests can simulate an in-flight scrape.
type fakeScraper struct {
	mu    sync.Mutex
	calls int
	gate  chan struct{}
}

func (f *fakeScraper) Collect(ctx context.Context) ([]storage.Metric, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
		}
	}
	return nil, nil
}

func (f *fakeScraper) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeStore records rollup and purge invocations while satisfying the full
// MetricsStore interface via the embedded nil interface.
type fakeStore struct {
	storage.MetricsStore
	mu            sync.Mutex
	rollups       []string
	purges        int
	lastRetention storage.Retention
}

func (f *fakeStore) Rollup(ctx context.Context, resolution string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollups = append(f.rollups, resolution)
	return nil
}

func (f *fakeStore) Purge(ctx context.Context, retention storage.Retention) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purges++
	f.lastRetention = retention
	return nil
}

func (f *fakeStore) rollupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rollups)
}

func TestScrapeLoopTicksAtInterval(t *testing.T) {
	scraper := &fakeScraper{}
	d := &Daemon{
		client:    &fakePinger{},
		scraper:   scraper,
		backoffFn: func(int) time.Duration { return time.Millisecond },
		stopped:   make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tick := make(chan time.Time)
	d.goLoop(func() { runScrapeLoop(ctx, d, tick) })

	tick <- time.Now()
	require.Eventually(t, func() bool {
		return scraper.count() == 1 && !d.scraping.Load()
	}, time.Second, 5*time.Millisecond)

	tick <- time.Now()
	require.Eventually(t, func() bool {
		return scraper.count() == 2 && !d.scraping.Load()
	}, time.Second, 5*time.Millisecond)
}

func TestScrapeLoopSkipsOverlappingTick(t *testing.T) {
	scraper := &fakeScraper{gate: make(chan struct{})}
	d := &Daemon{
		client:    &fakePinger{},
		scraper:   scraper,
		backoffFn: func(int) time.Duration { return time.Millisecond },
		stopped:   make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tick := make(chan time.Time)
	d.goLoop(func() { runScrapeLoop(ctx, d, tick) })

	tick <- time.Now() // first scrape starts and blocks on the gate
	require.Eventually(t, func() bool { return scraper.count() == 1 }, time.Second, 5*time.Millisecond)

	tick <- time.Now() // previous scrape still in flight: tick must be dropped
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, scraper.count(), "tick during in-flight scrape must be dropped")

	close(scraper.gate) // release the in-flight scrape
	require.Eventually(t, func() bool { return !d.scraping.Load() }, time.Second, 5*time.Millisecond)

	tick <- time.Now() // loop is free again: the next tick scrapes
	require.Eventually(t, func() bool {
		return scraper.count() == 2 && !d.scraping.Load()
	}, time.Second, 5*time.Millisecond)
}

func TestScrapeLoopStopsOnCancel(t *testing.T) {
	scraper := &fakeScraper{}
	d := &Daemon{scraper: scraper, stopped: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())

	d.goLoop(func() { runScrapeLoop(ctx, d, make(chan time.Time)) })
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scrape loop did not stop on context cancel")
	}
}

func TestScheduleRollupRegistersHourlyAndDaily(t *testing.T) {
	c := cron.New()
	d := &Daemon{store: &fakeStore{}}
	require.NoError(t, scheduleRollup(c, d, context.Background()))
	require.Len(t, c.Entries(), 2)

	known := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	nexts := map[time.Time]bool{}
	for _, e := range c.Entries() {
		nexts[e.Schedule.Next(known)] = true
	}
	assert.True(t, nexts[time.Date(2026, 8, 12, 10, 5, 0, 0, time.UTC)], "hourly rollup must be scheduled at minute 5")
	assert.True(t, nexts[time.Date(2026, 8, 13, 0, 10, 0, 0, time.UTC)], "daily rollup must be scheduled at 00:10")
}

func TestRollupOnceRunsRollupAndPurge(t *testing.T) {
	store := &fakeStore{}
	d := &Daemon{store: store}

	d.rollupOnce(context.Background(), "hourly")

	require.Len(t, store.rollups, 1)
	assert.Equal(t, "hourly", store.rollups[0])
	assert.Equal(t, 1, store.purges)
	assert.Equal(t, 24*time.Hour, store.lastRetention.Raw)
	assert.Equal(t, 90*24*time.Hour, store.lastRetention.Hourly)
	assert.Equal(t, 365*24*time.Hour, store.lastRetention.Daily)
}

func TestRollupLoopRunsJobsUntilCanceled(t *testing.T) {
	store := &fakeStore{}
	d := &Daemon{store: store}

	c := cron.New()
	_, err := c.AddFunc("@every 1s", func() { d.rollupOnce(context.Background(), "hourly") })
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runRollupLoop(ctx, d, c)
		close(done)
	}()

	// The @every 1s job must fire repeatedly at its cadence.
	require.Eventually(t, func() bool { return store.rollupCount() >= 2 }, 3*time.Second, 50*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("rollup loop did not stop on context cancel")
	}
}
