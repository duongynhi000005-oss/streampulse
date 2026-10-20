// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package daemon

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/pulsedev/streampulse/internal/alerts"
	"github.com/pulsedev/streampulse/internal/alerts/notify"
	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingNotifier captures notifications for assertions.
type recordingNotifier struct {
	mu            sync.Mutex
	notifications []alerts.Notification
}

func (n *recordingNotifier) Notify(ctx context.Context, note alerts.Notification) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notifications = append(n.notifications, note)
	return nil
}

func (n *recordingNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.notifications)
}

func alertTestGauge() *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "streampulse_alerts_firing",
		Help: "Alerts currently firing, by rule.",
	}, []string{"rule"})
}

// alertGaugeValue gathers the gauge registry and returns the value of the
// series for the given rule (0 when no series exists yet).
func alertGaugeValue(t *testing.T, reg *prometheus.Registry, rule string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != "streampulse_alerts_firing" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "rule" && l.GetValue() == rule {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

func lagBatch(v float64) []storage.Metric {
	return []storage.Metric{{
		EntityType: "consumer_group", EntityName: "orders-processor",
		Metric: "kafka.group.lag", Value: v,
	}}
}

func alertTestEngine(t *testing.T, store storage.MetricsStore, notifiers ...alerts.Notifier) *alerts.Engine {
	t.Helper()
	cond, err := alerts.ParseCondition("lag > 1000")
	require.NoError(t, err)
	engine := alerts.New([]alerts.Rule{{
		Name: "consumer-lag", Severity: "warning", Condition: cond,
		EntityType: "consumer_group", For: 0, RepeatInterval: time.Hour,
	}}, store)
	engine.SetNotifiers(notifiers)
	return engine
}

// TestAlertLoopFiresAndSyncsGauge drives the loop with an injected tick and
// asserts a firing rule notifies and sets the gauge to 1, then resolution
// clears it and sends the resolved notification.
func TestAlertLoopFiresAndSyncsGauge(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	notifier := &recordingNotifier{}
	engine := alertTestEngine(t, store, notifier)

	latest := &atomic.Pointer[[]storage.Metric]{}
	batch := lagBatch(5000)
	latest.Store(&batch)

	gauge := alertTestGauge()
	reg := prometheus.NewRegistry()
	reg.MustRegister(gauge)
	d := &Daemon{store: store, stats: NewScrapeStats()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		runAlertLoopTicks(ctx, d, engine, latest, gauge, tick)
		close(done)
	}()

	// Firing: lag 5000 > 1000, For 0 → immediate notify + gauge 1.
	tick <- time.Now()
	require.Eventually(t, func() bool {
		return notifier.count() == 1 &&
			alertGaugeValue(t, reg, "consumer-lag") == 1
	}, time.Second, 5*time.Millisecond)

	// Resolved: condition false → gauge 0 + resolved notification.
	batch2 := lagBatch(10)
	latest.Store(&batch2)
	tick <- time.Now()
	require.Eventually(t, func() bool {
		return notifier.count() == 2 &&
			alertGaugeValue(t, reg, "consumer-lag") == 0
	}, time.Second, 5*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("alert loop did not stop on context cancel")
	}
}

// TestAlertLoopToleratesNilLatest verifies a tick before the first
// successful scrape does not panic and leaves the rule silent.
func TestAlertLoopToleratesNilLatest(t *testing.T) {
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	notifier := &recordingNotifier{}
	engine := alertTestEngine(t, store, notifier)

	latest := &atomic.Pointer[[]storage.Metric]{}
	gauge := alertTestGauge()
	reg := prometheus.NewRegistry()
	reg.MustRegister(gauge)
	d := &Daemon{store: store}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		runAlertLoopTicks(ctx, d, engine, latest, gauge, tick)
		close(done)
	}()

	tick <- time.Now()
	require.Eventually(t, func() bool {
		return notifier.count() == 0 &&
			alertGaugeValue(t, reg, "consumer-lag") == 0
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, 0, notifier.count())
}

// TestNotifiersFromChannels asserts one notifier per unique channel is built,
// deduplicated across rules, with the constructor matching the channel type.
func TestNotifiersFromChannels(t *testing.T) {
	rules := []config.AlertRule{
		{Name: "a", Notify: []config.AlertChannel{
			{Type: "slack", Webhook: "https://hooks.slack.com/x"},
			{Type: "email", To: "a@b.c, d@e.f"},
		}},
		{Name: "b", Notify: []config.AlertChannel{
			{Type: "slack", Webhook: "https://hooks.slack.com/x"},
		}},
		{Name: "c", Notify: []config.AlertChannel{
			{Type: "pagerduty", Webhook: "PD-KEY"},
		}},
	}

	ns := notifiersFromChannels(rules)
	require.Len(t, ns, 3, "duplicate slack channel must be built once")

	types := map[string]bool{}
	for _, n := range ns {
		switch n.(type) {
		case *notify.Slack:
			types["slack"] = true
		case *notify.Email:
			types["email"] = true
		case *notify.PagerDuty:
			types["pagerduty"] = true
		}
	}
	assert.Equal(t, map[string]bool{"slack": true, "email": true, "pagerduty": true}, types)
}
