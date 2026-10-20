// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package daemon

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/pulsedev/streampulse/internal/alerts"
	"github.com/pulsedev/streampulse/internal/storage"
)

// alertInterval is the alert engine evaluation cadence (daemon.md: every
// 10s, with the last scrape's metrics).
const alertInterval = 10 * time.Second

// runAlertLoop evaluates the alert engine every alertInterval against the
// latest successful scrape batch and syncs the per-rule firing gauges from
// the engine state. A tick before any successful scrape evaluates an empty
// batch: rules without broker metrics stay silent or count toward
// scrape-failing, matching the engine's data-missing semantics.
func runAlertLoop(ctx context.Context, d *Daemon, engine *alerts.Engine, latest *atomic.Pointer[[]storage.Metric], gauge *prometheus.GaugeVec) {
	ticker := time.NewTicker(alertInterval)
	defer ticker.Stop()
	runAlertLoopTicks(ctx, d, engine, latest, gauge, ticker.C)
}

// runAlertLoopTicks is runAlertLoop with the tick channel injected so tests
// drive the cadence.
func runAlertLoopTicks(ctx context.Context, d *Daemon, engine *alerts.Engine, latest *atomic.Pointer[[]storage.Metric], gauge *prometheus.GaugeVec, tick <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			batch := latest.Load()
			if batch == nil {
				empty := make([]storage.Metric, 0)
				batch = &empty
			}
			if err := engine.Evaluate(ctx, *batch, time.Now()); err != nil {
				slog.Error("alert engine", "err", err)
			}
			syncAlertGauges(gauge, engine)
		}
	}
}

// syncAlertGauges sets each rule's firing gauge: 1 while the rule is firing,
// 0 otherwise.
func syncAlertGauges(gauge *prometheus.GaugeVec, engine *alerts.Engine) {
	for _, rule := range engine.Rules() {
		state, ok := engine.State(rule.Name)
		if !ok {
			continue
		}
		value := 0.0
		if state.Status == alerts.StateFiring {
			value = 1
		}
		gauge.WithLabelValues(rule.Name).Set(value)
	}
}
