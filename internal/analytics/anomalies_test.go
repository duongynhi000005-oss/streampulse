// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package analytics

import (
	"context"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seededAnomalyStore seeds 4 weeks of hourly kafka.group.lag points for g1: a
// baseline alternating 95/105 around 100, then a 500 spike in the last two
// hours. The baseline variance keeps the standard deviation non-zero so the
// spike scores z >> 4 (an all-identical baseline would hit the zero-std guard
// and suppress the signal entirely).
func seededAnomalyStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store := seededStore(t)
	t0 := time.Now().UTC().Truncate(time.Hour)
	metrics := make([]storage.Metric, 0, 4*7*24)
	for h := -(4*7*24 - 1); h <= -3; h++ {
		v := 105.0
		if h%2 == 0 {
			v = 95.0
		}
		metrics = append(metrics, storage.Metric{
			TS:         t0.Add(time.Duration(h) * time.Hour),
			ClusterID:  "local-dev",
			Metric:     scraper.MetricGroupLag,
			EntityType: "consumer_group",
			EntityName: "g1",
			Value:      v,
		})
	}
	metrics = append(metrics,
		storage.Metric{TS: t0.Add(-2 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 500},
		storage.Metric{TS: t0.Add(-1 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 500},
	)
	require.NoError(t, store.WriteBatch(context.Background(), metrics))
	require.NoError(t, store.Rollup(context.Background(), "hourly"))
	return store
}

// seededEmptyStore is a store with no metrics at all.
func seededEmptyStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	return seededStore(t)
}

// ─── Anomalies ─────────────────────────────────────────────────────────────

func TestAnomaliesDetectsSpike(t *testing.T) {
	store := seededAnomalyStore(t) // 4 weeks of kafka.group.lag for g1, last 2 hours = 500
	a := &Analyzer{store: store, client: nil}

	anoms, err := a.Anomalies(context.Background(), []string{scraper.MetricGroupLag}, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, anoms, 2) // the two spike hours
	for _, an := range anoms {
		assert.Equal(t, "g1", an.Entity)
		assert.Equal(t, "high", an.Direction)
		assert.Equal(t, "warning", an.Severity) // single threshold: |z| >= 2
		assert.Greater(t, an.ZScore, 4.0)
	}

	// insufficient history → no anomalies, no error
	empty, err := (&Analyzer{store: seededEmptyStore(t)}).Anomalies(context.Background(), []string{scraper.MetricGroupLag}, 7*24*time.Hour)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestAnomaliesSkipsNonRequestedMetrics(t *testing.T) {
	store := seededAnomalyStore(t) // only kafka.group.lag seeded
	a := &Analyzer{store: store, client: nil}

	anoms, err := a.Anomalies(context.Background(), []string{scraper.MetricTopicMsgRate}, 7*24*time.Hour)
	require.NoError(t, err)
	assert.Empty(t, anoms)
}

func TestAnomaliesRollingFallback(t *testing.T) {
	// Three hourly points only: too short for a seasonal baseline (needs ≥ 3
	// prior samples in the same hour-of-week bucket), so the spike must be
	// flagged through the rolling window of the preceding points.
	store := seededStore(t)
	t0 := time.Now().UTC().Truncate(time.Hour)
	metrics := []storage.Metric{
		{TS: t0.Add(-3 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 95},
		{TS: t0.Add(-2 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 105},
		{TS: t0.Add(-1 * time.Hour), ClusterID: "local-dev", Metric: scraper.MetricGroupLag, EntityType: "consumer_group", EntityName: "g1", Value: 500},
	}
	require.NoError(t, store.WriteBatch(context.Background(), metrics))
	require.NoError(t, store.Rollup(context.Background(), "hourly"))

	a := &Analyzer{store: store, client: nil}
	anoms, err := a.Anomalies(context.Background(), []string{scraper.MetricGroupLag}, 7*24*time.Hour)
	require.NoError(t, err)
	require.Len(t, anoms, 1)
	assert.Equal(t, "high", anoms[0].Direction)
	assert.Greater(t, anoms[0].ZScore, 4.0)
	assert.Equal(t, 500.0, anoms[0].Value)
}

func TestAnomaliesInvalidWindow(t *testing.T) {
	a := &Analyzer{store: seededStore(t), client: nil}
	_, err := a.Anomalies(context.Background(), []string{scraper.MetricGroupLag}, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "window")
}
