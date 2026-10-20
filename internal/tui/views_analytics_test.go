// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package tui

import (
	"context"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pulsedev/streampulse/internal/analytics"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seededAnalyticsStore opens a fresh in-memory store with daily growth data
// for orders and payments (pattern mirrors the analytics package tests).
func seededAnalyticsStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.NewSQLiteStore(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	metrics := []storage.Metric{
		{TS: t0.Add(-48 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 0},
		{TS: t0.Add(-24 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 86400},
		{TS: t0, ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "orders", Value: 172800},
		{TS: t0.Add(-24 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "payments", Value: 1000},
		{TS: t0, ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: "payments", Value: 2000},
	}
	require.NoError(t, store.WriteBatch(ctx, metrics))
	require.NoError(t, store.Rollup(ctx, "hourly"))
	require.NoError(t, store.Rollup(ctx, "daily"))
	return store
}

// seedAnalyticsTopic adds one more day of data for a topic and re-rolls up.
func seedAnalyticsTopic(t *testing.T, store *storage.SQLiteStore, topic string) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	require.NoError(t, store.WriteBatch(ctx, []storage.Metric{
		{TS: t0.Add(-24 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: topic, Value: 0},
		{TS: t0, ClusterID: "local-dev", Metric: scraper.MetricTopicMessages, EntityType: "topic", EntityName: topic, Value: 500},
	}))
	require.NoError(t, store.Rollup(ctx, "hourly"))
	require.NoError(t, store.Rollup(ctx, "daily"))
}

func TestRefreshAnalyticsComputesGrowthFromStore(t *testing.T) {
	store := seededAnalyticsStore(t)
	m := NewModelWithStore(store)
	base := time.Now()
	m.now = func() time.Time { return base }

	m.refreshAnalytics(context.Background())

	require.NoError(t, m.analyticsErr)
	require.Len(t, m.analytics, 2, "orders and payments expected")
	assert.Equal(t, "orders", m.analytics[0].Topic, "top growth report is the fastest growing topic")
	assert.NotEmpty(t, m.analytics[0].Sparkline)
	assert.Equal(t, base, m.analyticsUpdated)
}

func TestRefreshAnalyticsCacheStaleAfter30s(t *testing.T) {
	store := seededAnalyticsStore(t)
	m := NewModelWithStore(store)
	base := time.Now()
	m.now = func() time.Time { return base }

	m.refreshAnalytics(context.Background())
	require.Len(t, m.analytics, 2)
	require.Equal(t, base, m.analyticsUpdated)

	// New data lands, but the cache is still fresh: nothing recomputed.
	seedAnalyticsTopic(t, store, "audit")
	m.now = func() time.Time { return base.Add(29 * time.Second) }
	m.refreshAnalytics(context.Background())
	assert.Len(t, m.analytics, 2, "cache must be served before 30s elapsed")
	assert.Equal(t, base, m.analyticsUpdated, "analyticsUpdated must not move while cached")

	// Cache stale: recompute picks up the new topic.
	m.now = func() time.Time { return base.Add(31 * time.Second) }
	m.refreshAnalytics(context.Background())
	require.Len(t, m.analytics, 3, "recompute after 30s must see the new topic")
	assert.Equal(t, base.Add(31*time.Second), m.analyticsUpdated)
}

func TestRefreshAnalyticsErrorKeepsLastData(t *testing.T) {
	store := seededAnalyticsStore(t)
	m := NewModelWithStore(store)
	base := time.Now()
	m.now = func() time.Time { return base }
	m.refreshAnalytics(context.Background())
	require.NoError(t, m.analyticsErr)
	require.Len(t, m.analytics, 2)

	// A cluster connection joins the model; skew fails against an
	// unreachable broker, but growth data must be preserved and shown.
	m.kafkaClient = kafka.NewClient([]string{"127.0.0.1:1"})
	m.now = func() time.Time { return base.Add(31 * time.Second) }
	m.refreshAnalytics(context.Background())

	require.Error(t, m.analyticsErr, "skew against an unreachable broker must error")
	require.Len(t, m.analytics, 2, "last growth data must be kept on error")

	m.ready = true
	m.width = 120
	m.activeTab = 5
	view := m.renderAnalyticsView()
	assert.Contains(t, view, "skew", "error line names the failing report")
	assert.Contains(t, view, "orders", "kept growth data still renders")
	assert.Contains(t, view, "▁", "sparkline renders despite the error")
}

func TestAnalyticsBracketsCycleSelection(t *testing.T) {
	m := NewModelWithStore(nil)
	m.analytics = []analytics.GrowthReport{{Topic: "orders"}, {Topic: "payments"}}
	m.activeTab = 5

	tm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("]")})
	m = tm.(*Model)
	assert.Equal(t, 1, m.selectedTopic)

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("]")})
	m = tm.(*Model)
	assert.Equal(t, 0, m.selectedTopic, "] wraps past the last topic")

	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("[")})
	m = tm.(*Model)
	assert.Equal(t, 1, m.selectedTopic, "[ wraps past the first topic")

	// [ / ] on other tabs must not move the analytics selection.
	m.activeTab = 1
	tm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("]")})
	m = tm.(*Model)
	assert.Equal(t, 1, m.selectedTopic)
}

func TestAnalyticsViewNoData(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.width = 120
	m.activeTab = 5

	view := m.renderAnalyticsView()
	assert.Contains(t, view, "No growth detected")
	assert.Contains(t, view, "No anomalies detected")
	assert.Contains(t, view, "No rebalances in window")
}

func TestAnalyticsViewRendersGrowthSparkline(t *testing.T) {
	store := seededAnalyticsStore(t)
	m := NewModelWithStore(store)
	m.now = func() time.Time { return time.Now() }
	m.refreshAnalytics(context.Background())

	m.ready = true
	m.width = 120
	m.activeTab = 5
	m.selectedTopic = 1

	view := m.renderAnalyticsView()
	assert.Contains(t, view, "orders")
	assert.Contains(t, view, "payments")
	assert.Contains(t, view, "▁")
	assert.Contains(t, view, "█", "sparkline renders across the glyph range")
	assert.Contains(t, view, "msgs/s")
}

func TestAnalyticsViewRendersSkewAndRetention(t *testing.T) {
	m := NewModelWithStore(nil)
	m.ready = true
	m.width = 120
	m.activeTab = 5
	m.skew = []analytics.SkewReport{{
		Leaders:  map[string]int{"0": 12, "1": 5},
		Ratio:    1.8,
		Balanced: false,
	}}
	m.retention = []analytics.RetentionReport{{
		Topic:            "orders",
		RetentionMS:      7 * 24 * time.Hour,
		EstimateFillDays: 3,
		AtRisk:           true,
	}}

	view := m.renderAnalyticsView()
	assert.Contains(t, view, "PARTITION SKEW")
	assert.Contains(t, view, "12")
	assert.Contains(t, view, "1.80")
	assert.Contains(t, view, "UNBALANCED")
	assert.Contains(t, view, "RETENTION")
	assert.Contains(t, view, "orders")
	assert.Contains(t, view, "at risk")
}
