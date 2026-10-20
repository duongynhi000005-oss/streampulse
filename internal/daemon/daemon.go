// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

// Package daemon implements `streampulse serve`: a 24/7 process that scrapes
// Kafka metrics, persists and rolls them up, and exposes a Prometheus
// endpoint, all sharing one lifecycle with graceful shutdown.
package daemon

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulsedev/streampulse/internal/alerts"
	"github.com/pulsedev/streampulse/internal/alerts/notify"
	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/robfig/cron/v3"
)

// shutdownGrace bounds how long Shutdown waits for loops and the HTTP server
// (daemon.md lifecycle step 6).
const shutdownGrace = 10 * time.Second

// Scraper is the scraping engine the daemon drives once per interval. The
// daemon persists the collected batch itself so the same batch feeds the
// alert engine.
type Scraper interface {
	Collect(ctx context.Context) ([]storage.Metric, error)
}

// Client is the subset of the kafka client the daemon uses directly: it
// verifies broker connectivity before each scrape cycle.
type Client interface {
	Ping(ctx context.Context) error
}

// Daemon hosts the scrape, rollup, and alert loops and the Prometheus
// endpoint. Run blocks until the context is canceled or Shutdown is called.
type Daemon struct {
	cfg     *config.Config
	store   storage.MetricsStore
	client  Client
	scraper Scraper

	stats *ScrapeStats
	prom  *PromServer

	// latest is the most recent successful scrape batch, shared with the
	// alert loop (daemon.md: alerts evaluate the last scrape's metrics).
	latest *atomic.Pointer[[]storage.Metric]
	// alerts is the alert engine; nil when alerts are disabled.
	alerts *alerts.Engine

	scraping  atomic.Bool // in-flight guard for the scrape loop
	backoffFn func(int) time.Duration

	mu      sync.Mutex // guards cancel
	cancel  context.CancelFunc
	stopped chan struct{} // closed by Run once all loops have exited
	once    sync.Once
	wg      sync.WaitGroup
}

// Options configures the daemon.
type Options struct {
	// Version is the daemon build version, exposed via
	// streampulse_build_info on the Prometheus endpoint.
	Version string
}

// New creates a Daemon around the store and kafka client. The client is
// borrowed: the caller owns it and is responsible for Close.
func New(cfg *config.Config, store storage.MetricsStore, client *kafka.Client) *Daemon {
	return NewWithOptions(cfg, store, client, Options{})
}

// NewWithOptions creates a Daemon with explicit options.
func NewWithOptions(cfg *config.Config, store storage.MetricsStore, client *kafka.Client, opts Options) *Daemon {
	interval, _ := cfg.ParseScrapeInterval()
	stats := NewScrapeStats()
	d := &Daemon{
		cfg:       cfg,
		store:     store,
		client:    client,
		scraper:   scraper.New(cfg.ClusterID, client, store, interval),
		stats:     stats,
		prom:      NewPromServer(&cfg.Prometheus, stats, PromOptions{Version: opts.Version}),
		backoffFn: exponentialBackoff,
		stopped:   make(chan struct{}),
		latest:    &atomic.Pointer[[]storage.Metric]{},
	}
	d.alerts = newAlertEngine(cfg, store)
	return d
}

// newAlertEngine compiles the daemon's alert engine from config: built-in
// rules merged with config overrides, and one notifier per configured
// channel. A config error (unknown rule name, bad condition) is logged and
// falls back to the built-in rules so the daemon keeps running; config
// validation is the strict path.
func newAlertEngine(cfg *config.Config, store storage.MetricsStore) *alerts.Engine {
	rules, err := alerts.MergeRules(alerts.BuiltinRules(), cfg.Alerts)
	if err != nil {
		slog.Error("alert rules", "err", err)
		rules = alerts.BuiltinRules()
	}
	engine := alerts.New(rules, store)
	if notifiers := notifiersFromChannels(cfg.Alerts); len(notifiers) > 0 {
		engine.SetNotifiers(notifiers)
	}
	return engine
}

// notifiersFromChannels builds one notifier per configured alert channel
// across all rules, deduplicated by target. Secret indirection through env
// vars (webhook_env / password_env style fields) is resolved by the notifier
// constructors at build time.
func notifiersFromChannels(rules []config.AlertRule) []alerts.Notifier {
	var out []alerts.Notifier
	seen := make(map[string]bool)
	for _, r := range rules {
		for _, ch := range r.Notify {
			key := ch.Type + "\x00" + ch.Webhook + "\x00" + ch.Channel + "\x00" + ch.To
			if seen[key] {
				continue
			}
			seen[key] = true
			switch ch.Type {
			case "slack":
				out = append(out, notify.NewSlack(ch.Webhook))
			case "pagerduty":
				out = append(out, notify.NewPagerDuty(ch.Webhook, ""))
			case "email":
				out = append(out, notify.NewEmail(notify.SMTPConfig{To: splitRecipients(ch.To)}))
			}
		}
	}
	return out
}

// splitRecipients splits a comma-separated recipient list, trimming blanks.
func splitRecipients(s string) []string {
	var out []string
	for _, r := range strings.Split(s, ",") {
		if r = strings.TrimSpace(r); r != "" {
			out = append(out, r)
		}
	}
	return out
}

// Run blocks until ctx is canceled (or Shutdown is called), then waits for
// all loops to exit and returns nil. The Prometheus endpoint starts on entry
// and is shut down by Shutdown.
func (d *Daemon) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	d.mu.Lock()
	d.cancel = cancel
	d.mu.Unlock()

	if d.prom != nil {
		if err := d.prom.Start(); err != nil {
			slog.Error("prometheus endpoint", "err", err)
		}
	}
	d.startLoops(runCtx) // registers all loop goroutines synchronously
	d.wg.Wait()          // loops exit when runCtx is canceled
	close(d.stopped)
	return nil
}

// Shutdown cancels the loop context and waits for the loops within the
// shutdown grace period. It is idempotent and safe to call from a
// second-signal handler.
func (d *Daemon) Shutdown() error {
	d.once.Do(func() {
		d.mu.Lock()
		cancel := d.cancel
		d.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if d.prom != nil {
			_ = d.prom.Shutdown(ctx)
		}
		select {
		case <-d.stopped:
		case <-time.After(shutdownGrace):
		}
	})
	return nil
}

// goLoop launches a daemon loop, registering it with the WaitGroup
// synchronously so Shutdown's wait can never race loop startup.
func (d *Daemon) goLoop(fn func()) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		fn()
	}()
}

// startLoops launches the daemon's background loops.
func (d *Daemon) startLoops(ctx context.Context) {
	interval, err := d.cfg.ParseScrapeInterval()
	if err != nil {
		slog.Error("invalid scrape interval, using 5s", "err", err)
		interval = 5 * time.Second
	}

	d.goLoop(func() { runScrapeLoop(ctx, d, time.NewTicker(interval).C) })
	d.goLoop(func() { runRollupLoop(ctx, d, cron.New()) })

	if d.alerts != nil && d.stats != nil {
		d.goLoop(func() { runAlertLoop(ctx, d, d.alerts, d.latest, d.stats.AlertFiring) })
	}
}
